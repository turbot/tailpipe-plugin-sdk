package collection_state

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"log/slog"
	"os"
	"sync"
	"time"
)

// TODO refactor as propert base for API and artifactr colleciton state

type CollectionStateImpl[T config.Config] struct {
	Mut sync.RWMutex `json:"-"`
	// the time range of the data in the bucket
	StartTime time.Time `json:"start_time,omitempty"`
	EndTime   time.Time `json:"end_time,omitempty"`

	// for  end boundary we store the metadata
	// each time the end time changes, we must clear the map
	EndObjects map[string]struct{} `json:"end_objects,omitempty"`

	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	Granularity time.Duration `json:"granularity,omitempty"`

	//HasContinuation   bool                        `json:"has_continuation"`
	//ContinuationToken *string                     `json:"continuation_token,omitempty"`
	//
	//IsChronological bool `json:"is_chronological"`

	// path to the serialised collection state JSON
	jsonPath         string
	lastModifiedTime time.Time
	lastSaveTime     time.Time
}

// Init sets the filepath of the collection state and loads the state from the file if it exists
func (s *CollectionStateImpl[T]) Init(_ T, path string) error {
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

func (s *CollectionStateImpl[T]) SetGranularity(granularity time.Duration) {
	// TODO split into concept of accuracy AND granularity (better names)
	s.Granularity = granularity
}

func (s *CollectionStateImpl[T]) Save() error {
	s.Mut.Lock()
	defer s.Mut.Unlock()

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

	// write the JSON data to the file, overwriting any existing data
	err = os.WriteFile(s.jsonPath, jsonBytes, 0644)
	if err != nil {
		return fmt.Errorf("failed to write collection state to file: %w", err)
	}

	// update the last save time
	s.lastSaveTime = time.Now()

	return nil
}

func (s *CollectionStateImpl[T]) SetJSONPath(jsonPath string) {
	s.jsonPath = jsonPath
}

func (s *CollectionStateImpl[T]) GetGranularity() time.Duration {
	return s.Granularity
}

func (s *CollectionStateImpl[T]) IsEmpty() bool {
	return s.StartTime.IsZero()
}

// ShouldCollect returns whether the object should be collected
func (s *CollectionStateImpl[T]) ShouldCollect(m SourceItemMetadata) bool {
	timestamp := m.GetTimestamp()

	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we use start objects to track everythinbg
	if s.Granularity == 0 {
		// if we do not have a granularity we only use the start map
		return !s.endObjectsContain(m)
	}

	// if the time is between the start and end time (inclusive) we should NOT collect
	// (as have already collected it- assuming consistent artifact ordering)
	if timestamp.Compare(s.StartTime) >= 0 && timestamp.Compare(s.EndTime) <= 0 {
		return false
	}

	// if the timer is <= the end time + granularity, we must check if we have already collected it
	// (as we have reached the limit of the granularity)
	if timestamp.Compare(s.EndTime.Add(s.Granularity)) <= 0 {
		return !s.endObjectsContain(m)
	}

	// so it before the current start time or after the current end time - we should collect
	return true
}

// OnCollected is called when an object has been collected - update the end time and end objects if needed
// Note: the object name is the full path to the object
func (s *CollectionStateImpl[T]) OnCollected(metadata SourceItemMetadata) error {
	s.Mut.Lock()
	defer s.Mut.Unlock()

	// store modified time
	s.lastModifiedTime = time.Now()

	itemTimestamp := metadata.GetTimestamp()

	// if start time is not set, set it now
	if s.StartTime.IsZero() {
		s.StartTime = itemTimestamp
	}

	// if this timestamp is BEFORE the start time, we must be recollecting with an earlier styart time
	// - clear collection state
	// NOTE: in future, we will be more intelligent about this this and support multiple time ranges for for now just reset
	if itemTimestamp.Before(s.StartTime) {
		s.StartTime = itemTimestamp
		// clear end time - it will be set by the logic below
		s.EndTime = time.Time{}
	}

	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we must collect everything
	if s.Granularity == 0 {
		s.EndObjects[metadata.Identifier()] = struct{}{}
		return nil
	}

	// if the timestamp is before the CURRENT end time, then there is an issue
	// - the end time represents the time which we THOUGHT we had collected all data up to
	// this may indicate that the granularity for the sourcre has been set incorrectly
	// (as well as representing the granularity of the time we can deduce from the object name, the granularity also
	// represents the maximum lateness in reporting that we expect from a source.
	// Thus if an API may report log entries up[ to 1 hour late, the granularity should be set to 1 hour
	// If it is set to 1 hour but then reports an entry 2 hours late, thids condition would occur
	if itemTimestamp.Before(s.EndTime) {
		// TODO perhaps we should just update the granularity?
		slog.Warn("Artifact timestamp is before the end time, i.e. the time up to which we believed we had collected all data - this may indicate an incorrect granularity setting", "granularity", s.Granularity, "item timestamp", itemTimestamp, "collection state end time", s.EndTime)
		return nil
	}

	// update our end times as needed
	// NOTE: the end time are adjusted by the granularity
	// i.e if the granularity is 1 hour, and the artifact time is 12:00:00,
	// we are sure we have collected ALL data up to 11:00
	// the end time will be 11:00:00,
	endTime := itemTimestamp.Add(-s.Granularity)
	if endTime.After(s.EndTime) || s.EndTime.IsZero() {
		s.SetEndTime(endTime)
	}

	// add the object to the end map
	s.EndObjects[metadata.Identifier()] = struct{}{}

	return nil
}

// SetEndTime overrides the base implementation to also clear the end objects
func (s *CollectionStateImpl[T]) SetEndTime(t time.Time) {
	s.EndTime = t
	// clear the end map
	s.EndObjects = make(map[string]struct{})
}

func (s *CollectionStateImpl[T]) GetStartTime() time.Time {
	return s.StartTime
}

func (s *CollectionStateImpl[T]) GetEndTime() time.Time {
	// i.e. the last time period we are sure we have ALL data for
	return s.EndTime
}

func (s *CollectionStateImpl[T]) endObjectsContain(m SourceItemMetadata) bool {
	s.Mut.RLock()
	defer s.Mut.RUnlock()

	_, ok := s.EndObjects[m.Identifier()]
	return ok
}
