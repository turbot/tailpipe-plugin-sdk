package collection_state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/parse"
)

// ReverseOrderCollectionState is the interface for the collection state of an S3 bucket
// return the start time and the end time for the data downloaded

type ReverseOrderCollectionState[T parse.Config] struct {
	// collection of time ranges ordered by time
	TimeRanges []*TimeRangeCollectionStateImpl `json:"time_ranges"`

	activeTimeRange *TimeRangeCollectionStateImpl

	granularity time.Duration

	// path to the serialised collection state JSON
	jsonPath         string
	lastModifiedTime time.Time
	lastSaveTime     time.Time

	mut *sync.RWMutex
}

func NewReverseOrderCollectionState[T parse.Config]() CollectionState[T] {
	return &ReverseOrderCollectionState[T]{
		//objectStateMap: make(map[string]*TimeRangeCollectionStateImpl),
		mut: &sync.RWMutex{},
	}
}

// Init sets the filepath of the collection state and loads the state from the file if it exists
func (s *ReverseOrderCollectionState[T]) Init(_ T, path string) error {
	s.jsonPath = path

	// if there is a file at the path, load it
	if _, err := os.Stat(path); err == nil {
		// TODO #err should we just warn and delete/rename the file
		// read the file
		jsonBytes, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read collection state file '%s': %w", path, err)
		}
		err = json.Unmarshal(jsonBytes, s)
		if err != nil {
			return fmt.Errorf("failed to unmarshal collection state file '%s': %w", path, err)
		}
	}
	return nil
}

func (s *ReverseOrderCollectionState[T]) Start() {
	// add a new time range
	s.activeTimeRange = NewTimeRangeCollectionStateImpl(CollectionOrderReverse)
	s.TimeRanges = append(s.TimeRanges, s.activeTimeRange)
}

func (s *ReverseOrderCollectionState[T]) End() {
	s.activeTimeRange = nil
}

// SetGranularity sets the granularity of the collection state - this is determined by the file layout and the
// granularity of the time metadata it contains
func (s *ReverseOrderCollectionState[T]) SetGranularity(granularity time.Duration) {
	s.granularity = granularity
}

// GetGranularity returns the granularity of the collection state
func (s *ReverseOrderCollectionState[T]) GetGranularity() time.Duration {
	return s.granularity
}

func (s *ReverseOrderCollectionState[T]) GetStartTime() time.Time {
	if len(s.TimeRanges) == 0 {
		return time.Time{}
	}
	// return the time of the first state
	return s.TimeRanges[0].GetStartTime()

}

func (s *ReverseOrderCollectionState[T]) GetEndTime() time.Time {
	if len(s.TimeRanges) == 0 {
		return time.Time{}
	}
	// return the time of the last state
	return s.TimeRanges[len(s.TimeRanges)-1].GetEndTime()
}

// SetEndTime sets the end time for the collection state - update all trunk states
// This is called when we are using the --from flag to force recollection
func (s *ReverseOrderCollectionState[T]) SetEndTime(newEndTime time.Time) {
	s.mut.Lock()
	defer s.mut.Unlock()

	// if before the first time range -> clear everything
	if newEndTime.Before(s.TimeRanges[0].GetStartTime()) {
		s.Clear()
		return
	}

	// if after the last time range -> do nothing
	if newEndTime.After(s.TimeRanges[len(s.TimeRanges)-1].GetEndTime()) {
		return
	}

	// if within a time range -> set the end time of the time range & discard subsequent time ranges
	// or if between two time ranges -> discard subsequent time ranges
	var newTimeRanges []*TimeRangeCollectionStateImpl
	for i, r := range s.TimeRanges {
		if !newEndTime.Before(r.firstEntryTime) && !newEndTime.After(r.endTime) {
			r.SetEndTime(newEndTime)
			newTimeRanges = append(newTimeRanges, r)
			break
		}

		if i+1 < len(s.TimeRanges) && newEndTime.After(r.endTime) && newEndTime.Before(s.TimeRanges[i+1].firstEntryTime) {
			newTimeRanges = append(newTimeRanges, r)
			break
		}

		newTimeRanges = append(newTimeRanges, r)
	}
	s.TimeRanges = newTimeRanges
}

func (s *ReverseOrderCollectionState[T]) Clear() {
	s.mut.Lock()
	defer s.mut.Unlock()
	// clear the map
	s.TimeRanges = nil
}

// ShouldCollect returns whether the object should be collected, based on the time metadata in the object
func (s *ReverseOrderCollectionState[T]) ShouldCollect(id string, timestamp time.Time) bool {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.activeTimeRange == nil {
		panic("Start must be called before we start collecting")
	}

	if timestamp.Compare(s.activeTimeRange.firstEntryTime) >= 0 {
		return s.activeTimeRange.ShouldCollect(id, timestamp)
	}

	if len(s.TimeRanges) > 1 {
		penultimateState := s.TimeRanges[len(s.TimeRanges)-2]

		if timestamp.Before(penultimateState.endTime.Add(s.granularity)) && timestamp.After(penultimateState.endTime) {
			// check if item is in the penultimate state end objects
			return penultimateState.ShouldCollect(id, timestamp)
		}

		if timestamp.Compare(penultimateState.endTime) <= 0 {
			s.mergeActiveRangeWithPrevious()

			return false
		}
	}
	return true
}

func (s *ReverseOrderCollectionState[T]) mergeActiveRangeWithPrevious() {
	// merge the last two time ranges
	// the last range is the active range
	// the penultimate range is the one before that
	penultimateState := s.TimeRanges[len(s.TimeRanges)-2]
	penultimateState.SetEndTime(s.activeTimeRange.GetEndTime())
	penultimateState.EndObjects = s.activeTimeRange.EndObjects
	// remove the last range
	s.TimeRanges = s.TimeRanges[:len(s.TimeRanges)-1]
	// set the active range to the penultimate range
	s.activeTimeRange = penultimateState

	s.lastModifiedTime = time.Now()
}

// OnCollected is called when an object has been collected - update our end time and end objects if needed
func (s *ReverseOrderCollectionState[T]) OnCollected(id string, timestamp time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.lastModifiedTime = time.Now()
	return s.activeTimeRange.OnCollected(id, timestamp)
}

// Save serialises the collection state to a JSON file
func (s *ReverseOrderCollectionState[T]) Save() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	slog.Info("Saving collection state", "lastModifiedTime", s.lastModifiedTime, "lastSaveTime", s.lastSaveTime)

	// if the last save time is after the last modified time, then we have nothing to do
	if s.lastSaveTime.After(s.lastModifiedTime) {
		slog.Info("collection state has not been modified since last save")
		// nothing to do
		return nil
	}

	slog.Info("We are actually saving the collection state")

	jsonBytes, err := json.Marshal(s)
	if err != nil {
		return err
	}
	// ensure the target file path is valid
	if s.jsonPath == "" {
		return fmt.Errorf("collection state path is not set")
	}

	// write the JSON data to the file, overwriting any existing data
	err = os.WriteFile(s.jsonPath, jsonBytes, 0644)
	if err != nil {
		return fmt.Errorf("failed to write collection state to file: %w", err)
	}

	// update the last save time
	s.lastSaveTime = time.Now()

	return nil
}

// IsEmpty returns whether the collection state is empty
func (s *ReverseOrderCollectionState[T]) IsEmpty() bool {
	for _, trunkState := range s.TimeRanges {
		if trunkState != nil {
			return false
		}
	}
	return true
}
