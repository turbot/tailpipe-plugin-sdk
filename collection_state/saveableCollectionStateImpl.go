package collection_state

import (
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"os"
	"sync"
	"time"
)

type SaveableCollectionStateImpl[T parse.Config] struct {
	collectionState CollectionState[T]
	// path to the serialised collection state JSON
	jsonPath     string
	lastSaveTime time.Time
	// the time the last artifact was collected
	// TACTICAL: this is used in GetEndTime called by RowSourceImpl.setFromTime
	// if there is no timing information in the files, we use this to determine the end time
	// which we pass to the CLI to use as the --from time (if one has not been passed)
	// NOTE: this assumes forward collection
	LastModifiedTime time.Time `json:"last_modified_time,omitempty"`

	mut *sync.RWMutex
}

func NewSaveableCollectionState[T parse.Config](state CollectionState[T]) *SaveableCollectionStateImpl[T] {
	return &SaveableCollectionStateImpl[T]{
		collectionState: state,
		mut:             &sync.RWMutex{},
	}
}

func (s *SaveableCollectionStateImpl[T]) Init(config T, path string) error {
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

	return s.collectionState.Init(config, path)
}

func (s *SaveableCollectionStateImpl[T]) SetGranularity(duration time.Duration) {
	s.collectionState.SetGranularity(duration)
}

func (s *SaveableCollectionStateImpl[T]) GetGranularity() time.Duration {
	return s.collectionState.GetGranularity()
}

func (s *SaveableCollectionStateImpl[T]) GetFromTime() time.Time {
	return s.collectionState.GetFromTime()
}

func (s *SaveableCollectionStateImpl[T]) GetToTime() time.Time {
	endTime := s.collectionState.GetToTime()
	// TODO IS THIS RIGHT???? NO
	// if there is NO end time, the end of the last collection
	if endTime.IsZero() {
		endTime = s.LastModifiedTime
	}
	return endTime
}

func (s *SaveableCollectionStateImpl[T]) OnCollectionStarted(fromTime time.Time, toTime time.Time) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.collectionState.OnCollectionStarted(fromTime, toTime)
}

func (s *SaveableCollectionStateImpl[T]) OnCollectionComplete() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// ensure we save the state
	s.LastModifiedTime = time.Now()

	// call the collection state complete method
	if err := s.collectionState.OnCollectionComplete(); err != nil {
		return fmt.Errorf("error completing collection state: %w", err)
	}

	// save the collection state
	if err := s.Save(); err != nil {
		return fmt.Errorf("error saving collection state: %w", err)
	}
	return nil
}

func (s *SaveableCollectionStateImpl[T]) ShouldCollect(id string, timestamp time.Time) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return s.collectionState.ShouldCollect(id, timestamp)
}

func (s *SaveableCollectionStateImpl[T]) OnCollected(id string, timestamp time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// store modified time to ensure we save the state
	s.LastModifiedTime = time.Now()

	return s.collectionState.OnCollected(id, timestamp)
}

func (s *SaveableCollectionStateImpl[T]) IsEmpty() bool {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.collectionState.IsEmpty()
}

func (s *SaveableCollectionStateImpl[T]) Save() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// if the last save time is after the last modified time, then we have nothing to do
	if s.lastSaveTime.After(s.LastModifiedTime) {
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
	err = os.WriteFile(s.jsonPath, jsonBytes, 0644) //nolint:gosec // 0644 is the file permission we want
	if err != nil {
		return fmt.Errorf("failed to write collection state to file: %w", err)
	}

	// update the last save time
	s.lastSaveTime = time.Now()

	return nil
}
