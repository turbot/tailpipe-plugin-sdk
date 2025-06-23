package collection_state

import (
	"encoding/json"
	"fmt"
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
	// TODO check this
	// TACTICAL: this is used in GetEndTime called by RowSourceImpl.setFromTime
	// if there is no timing information in the files, we use this to determine the end time
	// which we pass to the CLI to use as the --from time (if one has not been passed)
	// NOTE: this assumes forward collection
	LastModifiedTime time.Time `json:"last_modified_time,omitempty"`

	mut *sync.RWMutex
}

func NewSaveableCollectionState(state CollectionState) *SaveableCollectionState {
	return &SaveableCollectionState{
		State: state,
		mut:   &sync.RWMutex{},
	}
}

// Init initializes the SaveableCollectionState with a CollectionTimeRange and a file path
// if the file exists, it loads the state from the file
func (s *SaveableCollectionState) Init(collectionTimeRange *CollectionTimeRange, path string) error {
	s.jsonPath = path
	// if there is a file at the path, load it
	if _, err := os.Stat(path); err == nil {
		if err = s.LoadFromFile(path); err != nil {
			return err
		}
		// fall through to ensure the state is initialized
	}

	if err := s.State.Init(collectionTimeRange); err != nil {
		return fmt.Errorf("failed to initialize collection state: %w", err)
	}
	return s.State.Validate()
}

func (s *SaveableCollectionState) SetGranularity(duration time.Duration) {
	s.State.SetGranularity(duration)
}

func (s *SaveableCollectionState) GetGranularity() time.Duration {
	return s.State.GetGranularity()
}

func (s *SaveableCollectionState) GetFromTime() time.Time {
	return s.State.GetFromTime()
}

func (s *SaveableCollectionState) GetToTime() time.Time {
	endTime := s.State.GetToTime()
	// TODO #CS IS THIS RIGHT???? WHAT ABOUT NO GRANULARITY
	// if there is NO end time, the end of the last collection
	if endTime.IsZero() {
		endTime = s.LastModifiedTime
	}
	return endTime
}

func (s *SaveableCollectionState) OnCollectionComplete() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// ensure we save the state
	s.LastModifiedTime = time.Now()

	// call the collection state complete method
	if err := s.State.OnCollectionComplete(); err != nil {
		return fmt.Errorf("error completing collection state: %w", err)
	}

	// save the collection state
	if err := s.Save(); err != nil {
		return fmt.Errorf("error saving collection state: %w", err)
	}
	return nil
}

func (s *SaveableCollectionState) ShouldCollect(id string, timestamp time.Time) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return s.State.ShouldCollect(id, timestamp)
}

func (s *SaveableCollectionState) OnCollected(id string, timestamp time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// store modified time to ensure we save the state
	s.LastModifiedTime = time.Now()

	return s.State.OnCollected(id, timestamp)
}

func (s *SaveableCollectionState) IsEmpty() bool {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.State.IsEmpty()
}

// Save serializes the underlying collection state to JSON and writes it to the file specified by jsonPath.
func (s *SaveableCollectionState) Save() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// if the last save time is after the last modified time, then we have nothing to do
	if s.lastSaveTime.After(s.LastModifiedTime) {
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

func (s *SaveableCollectionState) Clear(timeRange CollectionTimeRange) {
	s.State.Clear(timeRange)
}
