package collection_state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
)

// ReverseOrderCollectionState is the interface for the collection state of an S3 bucket
// return the start time and the end time for the data downloaded

type ReverseOrderCollectionState[T artifact_source_config.ArtifactSourceConfig] struct {
	// collection of time ranges ordered by time
	timeRanges []*TimeRangeCollectionStateImpl

	granularity time.Duration

	// path to the serialised collection state JSON
	jsonPath         string
	lastModifiedTime time.Time
	lastSaveTime     time.Time

	mut *sync.RWMutex

}

func NewReverseOrderCollectionState[T artifact_source_config.ArtifactSourceConfig]() CollectionState[T] {
	return &ReverseOrderCollectionState[T]{
		//objectStateMap: make(map[string]*TimeRangeCollectionStateImpl),
		mut:            &sync.RWMutex{},
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
	s.timeRanges = append(s.timeRanges, NewTimeRangeCollectionStateImpl())
}


func (s *ReverseOrderCollectionState[T]) End() {

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
	if len(s.timeRanges) == 0 {
		return time.Time{}
	}
	// return the time of the first state
	return s.timeRanges[0].GetStartTime()

}

func (s *ReverseOrderCollectionState[T]) GetEndTime() time.Time {
	if len(s.timeRanges) == 0 {
		return time.Time{}
	}
	// return the time of the last state
	return s.timeRanges[len(s.timeRanges)-1].GetEndTime()
}

// SetEndTime sets the end time for the collection state - update all trunk states
// This is called when we are using the --from flag to force recollection
func (s *ReverseOrderCollectionState[T]) SetEndTime(newEndTime time.Time) {
	s.mut.Lock()
	defer s.mut.Unlock()

	// for each time range, determine if the new end time is within the range
	// if it is, set the end time and clear subsequent states
	var finalTimeRangeIdx int
	for i, timeRange := range s.timeRanges {
		if timeRange.GetStartTime().Before(newEndTime) && timeRange.GetEndTime().After(newEndTime) {
			timeRange.SetEndTime(newEndTime)
			finalTimeRangeIdx = i
		}
	}
	// TODO is this correct or withouth the plus 1?
	s.timeRanges = s.timeRanges[:finalTimeRangeIdx+1]
}

func (s *ReverseOrderCollectionState[T]) Clear() {
	s.mut.Lock()
	defer s.mut.Unlock()
	// cleat the map
	s.timeRanges = nil
}



// ShouldCollect returns whether the object should be collected, based on the time metadata in the object
func (s *ReverseOrderCollectionState[T]) ShouldCollect(id string, timestamp time.Time) bool {
	s.mut.Lock()
	defer s.mut.Unlock()

	if len(s.timeRanges) == 0 {
		panic("Start must be called before we start collecting")
	}

	// TODO active time range concept

	finalTimeRange := s.timeRanges[len(s.timeRanges)-1]
	if timestamp.After(finalTimeRange.StartTime) {
		return false
	}

	if len(s.timeRanges) > 1 {
		penultimateState := s.timeRanges[len(s.timeRanges)-2]
		if timestamp.Before(penultimateState.EndTime) {
			MERGE TIME RANGES
			Add function HasJustMergedWithPreviousTimeRange()

			return false
		}
	}
	return true
}

func HasJustMergedWithPreviousTimeRange(){
	merge ranges
	s.lastModifiedTime = time.Now()
}


// OnCollected is called when an object has been collected - update our end time and end objects if needed
func (s *ReverseOrderCollectionState[T]) OnCollected(id string, timestamp time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// ifd we just merged with prev range, do nothing
	if HasJustMergedWithPreviousTimeRange(){
		return nil
	}

	// otherwise sets start time

	// TODO
	// store modified time to ensure we save the state
	s.lastModifiedTime = time.Now()

	slog.Info("OnCollected", "id", id, "timestamp", timestamp, "lastModifiedTime", s.lastModifiedTime)
	// we should have stored a collection state mapping for this object
	collectionState, ok := s.objectStateMap[id]
	if !ok {
		return fmt.Errorf("no collection state mapping found for item '%s' - this should have been set in ShouldCollect", id)
	}

	return collectionState.OnCollected(id, timestamp)
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
	for _, trunkState := range s.timeRanges {
		if trunkState != nil {
			return false
		}
	}
	return true
}
