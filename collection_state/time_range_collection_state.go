package collection_state

import (
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"log/slog"
	"os"
	"sync"
	"time"
)

type TimeRangeCollectionState[T parse.Config] struct {
	TimeRangeCollectionStateImpl

	// path to the serialised collection state JSON
	jsonPath         string
	lastModifiedTime time.Time
	lastSaveTime     time.Time

	mut *sync.RWMutex
}

func NewTimeRangeCollectionState[T parse.Config](granularity time.Duration) CollectionState[T] {
	s := NewTimeRangeCollectionStateImpl(granularity)
	return &TimeRangeCollectionState[T]{
		TimeRangeCollectionStateImpl: *s,
	}
}

// Init sets the filepath of the collection state and loads the state from the file if it exists
func (s *TimeRangeCollectionState[T]) Init(_ T, path string) error {
	s.jsonPath = path

	// if there is a file at the path, load it
	if _, err := os.Stat(path); err == nil {
		// TODO #err should we just warn and delete/rename the file
		// read the file
		jsonBytes, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read collection state file: %w", err)
		}
		err = json.Unmarshal(jsonBytes, s)
		if err != nil {
			return fmt.Errorf("failed to unmarshal collection state file: %w", err)
		}
	}
	return nil
}

// ShouldCollect returns whether the object should be collected, based on the time metadata in the object
func (s *TimeRangeCollectionState[T]) ShouldCollect(m SourceItemMetadata) bool {
	s.mut.Lock()
	defer s.mut.Unlock()

	return s.TimeRangeCollectionStateImpl.ShouldCollect(m)
}

// OnCollected is called when an object has been collected - update our end time and end objects if needed
func (s *TimeRangeCollectionState[T]) OnCollected(metadata SourceItemMetadata) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// store modified time to ensure we save the state
	s.lastModifiedTime = time.Now()

	return s.TimeRangeCollectionStateImpl.OnCollected(metadata)
}

// SetGranularity sets the granularity of the collection state - this is determined by the file layout and the
// granularity of the time metadata it contains
func (s *TimeRangeCollectionState[T]) SetGranularity(granularity time.Duration) {
	s.Granularity = granularity
}

// GetGranularity returns the granularity of the collection state
func (s *TimeRangeCollectionState[T]) GetGranularity() time.Duration {
	return s.Granularity
}

// Save serialises the collection state to a JSON file
func (s *TimeRangeCollectionState[T]) Save() error {
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
