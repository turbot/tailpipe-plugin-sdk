package collection_state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// SaveableCollectionState is a decorator for CollectionState that allows it to be saved to disk
// It wraps a CollectionState and provides methods to save the state to a JSON file
// It also provides a mutex to ensure thread safety when accessing the collection state
type SaveableCollectionState struct {
	StructVersion int `json:"struct_version"`

	State CollectionState `json:"state"`
	// path to the serialised collection state JSON
	jsonPath     string
	lastSaveTime time.Time
	// the time the last artifact was collected
	lastModifiedTime time.Time
	granularity      time.Duration
	mut              *sync.RWMutex
}

func NewSaveableCollectionState(state CollectionState, path string) (*SaveableCollectionState, error) {
	s := &SaveableCollectionState{
		State: state,
		mut:   &sync.RWMutex{},
	}
	// set the path to the JSON file
	s.jsonPath = path
	// if there is a file at the path, load it
	if _, err := os.Stat(path); err == nil {
		if err = s.LoadFromFile(path); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Init initializes the SaveableCollectionState with a DirectionalTimeRange and a file path
// if the file exists, it loads the state from the file
func (s *SaveableCollectionState) Init(collectionTimeRange DirectionalTimeRange, recollect bool, granularity time.Duration) error {
	// save the granularity
	s.granularity = granularity

	// NOTE: if granularity is zero, we DO NOT support collecting for a time range so clear the time range
	if granularity == 0 {
		slog.Info("Granularity is zero - clearing collection time range")
		collectionTimeRange = DirectionalTimeRange{}
	}
	// if we are recollecting, clear BEFORE call to Init, as Init will set the active range
	// which we must not do until we have cleared the state
	if recollect {
		slog.Info("Recollecting data - clearing collection state for collection time range", "lower boundary time", collectionTimeRange.LowerBoundary, "upper boundary time", collectionTimeRange.UpperBoundary)
		// if we are recollecting, set the collection state to empty
		s.State.Clear(collectionTimeRange)
	}

	s.State.Init(collectionTimeRange, granularity)

	return s.State.Validate()
}

func (s *SaveableCollectionState) GetFromTime() time.Time {
	return s.State.GetFromTime()
}

func (s *SaveableCollectionState) GetToTime() time.Time {
	endTime := s.State.GetToTime()

	return endTime
}

func (s *SaveableCollectionState) OnCollectionComplete() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// ensure we save the state
	s.lastModifiedTime = time.Now()

	// call the collection state complete method
	if err := s.State.OnCollectionComplete(); err != nil {
		return fmt.Errorf("error completing collection state: %w", err)
	}

	// save the collection state
	// NOTE: call the non-exported version which does not lock the mutex
	if err := s.save(); err != nil {
		return fmt.Errorf("error saving collection state: %w", err)
	}
	return nil
}

func (s *SaveableCollectionState) ShouldCollect(id string, timestamp time.Time) bool {
	s.mut.Lock()
	defer s.mut.Unlock()

	return s.State.ShouldCollect(id, timestamp)
}

func (s *SaveableCollectionState) OnCollected(id string, timestamp time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// store modified time to ensure we save the state
	s.lastModifiedTime = time.Now()

	return s.State.OnCollected(id, timestamp)
}

func (s *SaveableCollectionState) IsEmpty() bool {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.State.IsEmpty()
}

// Save serializes the underlying collection state to JSON and writes it to the file specified by jsonPath.
// NOTE: This method locks the mutex to ensure thread safety.
func (s *SaveableCollectionState) Save() error {
	s.mut.Lock()
	defer s.mut.Unlock()
	// call the non-exported save method which does not lock the mutex
	return s.save()
}

// save serializes the collection state to JSON and writes it to the file specified by jsonPath.
// NOTE: This method assumes the mutex is already locked by the caller.
func (s *SaveableCollectionState) save() error {
	// if the last save time is after the last modified time, then we have nothing to do
	if s.lastSaveTime.After(s.lastModifiedTime) {
		// nothing to do
		return nil
	}
	// ensure StructVersion is set
	s.StructVersion = CollectionStateStructVersion

	jsonBytes, err := json.Marshal(s)
	if err != nil {
		return err
	}
	// ensure the target file path is valid
	if s.jsonPath == "" {
		return fmt.Errorf("collection state path is not set")
	}

	// if we are empty, delete the file
	// NOTE: call underlying IsEmpty(), not our top level here, as it will lock the mutex and we are already holding it
	if s.State.IsEmpty() {
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

func (s *SaveableCollectionState) RegisterPath(path string, metadata map[string]string) {
	if cs, ok := s.State.(CollectionStateWithPaths); ok {
		cs.RegisterPath(path, metadata)
	}
}

func (s *SaveableCollectionState) LoadFromFile(path string) error {
	// read the file
	jsonBytes, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read collection state file '%s': %w", path, err)
	}

	// unmarshal the JSON data into the collection state
	err = json.Unmarshal(jsonBytes, s)
	if err != nil {
		return fmt.Errorf("failed to unmarshal collection state file '%s': %w", path, err)
	}
	if s.StructVersion == 0 {
		return s.State.MigrateFromLegacyState(jsonBytes)
	}
	return nil
}

func (s *SaveableCollectionState) GetGranularity() time.Duration {
	return s.granularity
}
