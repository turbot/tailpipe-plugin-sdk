package collection_state

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/parse"
)

type TimeRangeCollectionState[T parse.Config] struct {
	TimeRangeCollectionStateImpl

	// path to the serialised collection state JSON
	jsonPath         string
	lastModifiedTime time.Time
	lastSaveTime     time.Time

	mut *sync.RWMutex
}

func NewTimeRangeCollectionState[T parse.Config]() CollectionState[T] {
	s := NewTimeRangeCollectionStateImpl(CollectionOrderChronological)
	return &TimeRangeCollectionState[T]{
		TimeRangeCollectionStateImpl: *s,
		mut:                          &sync.RWMutex{},
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
			return fmt.Errorf("failed to read collection state file '%s': %w", path, err)
		}
		err = json.Unmarshal(jsonBytes, s)
		if err != nil {
			return fmt.Errorf("failed to unmarshal collection state file '%s': %w", path, err)
		}
	}
	return nil
}

// ShouldCollect returns whether the object should be collected, based on the time metadata in the object
func (s *TimeRangeCollectionState[T]) ShouldCollect(id string, timestamp time.Time) bool {
	s.mut.Lock()
	defer s.mut.Unlock()

	return s.TimeRangeCollectionStateImpl.ShouldCollect(id, timestamp)
}

// OnCollected is called when an object has been collected - update our end time and end objects if needed
func (s *TimeRangeCollectionState[T]) OnCollected(id string, timestamp time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// store modified time to ensure we save the state
	s.lastModifiedTime = time.Now()

	return s.TimeRangeCollectionStateImpl.OnCollected(id, timestamp)
}

// SetEndTime sets the end time for the collection state
// It may be called to set the end time to earlier than the current end time if a --from flag is used to force recollection
func (s *TimeRangeCollectionState[T]) SetEndTime(newEndTime time.Time) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.TimeRangeCollectionStateImpl.SetEndTime(newEndTime)
}

func (s *TimeRangeCollectionState[T]) Clear() {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.TimeRangeCollectionStateImpl.Clear()
}

// Save serialises the collection state to a JSON file
func (s *TimeRangeCollectionState[T]) Save() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// if the last save time is after the last modified time, then we have nothing to do
	if s.lastSaveTime.After(s.lastModifiedTime) {
		// nothing to do
		return nil
	}
	jsonBytes, err := json.Marshal(s)
	if err != nil {
		return err
	}
	// ensure the target file path is valid
	if s.jsonPath == "" {
		return fmt.Errorf("collection state path is not set")
	}

	// if we are empty, delete the file
	if s.IsEmpty() {
		err := os.Remove(s.jsonPath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete collection state file: %w", err)
		}
		return nil
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
