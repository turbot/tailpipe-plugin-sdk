package collection_state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

const MinArtifactGranularity = time.Hour * 24

// ArtifactCollectionStateImpl is the interface for the collection state of an S3 bucket
// return the start time and the end time for the data downloaded

type ArtifactCollectionStateImpl[T artifact_source_config.ArtifactSourceConfig] struct {
	// map of trunk paths to collection state for that trunk
	// a trunk is a path segment that does not contain any time metadata
	// for example if the path is s3://bucket/folder1/folder2/2021/01/01/file.txt then the trunk is s3://bucket/folder1/folder2
	TrunkStates map[string]*TimeRangeCollectionStateImpl `json:"trunk_states,omitempty"`

	// the time the last artifact was collected
	// TACTICAL: this is used in GetEndTime called by RowSourceImpl.setFromTime
	// if there is no timing information in the files, we use this to determine the end time
	// which we pass to the CLI to use as the --from time (if one has not been passed)
	// NOTE: this assumes forward collection
	LastModifiedTime time.Time `json:"last_modified_time,omitempty"`

	// map of object identifier to collection state which contains the object
	// used to store the collection state for each object between the ShouldCollect call and the OnCollected call
	// NOTE: the map entry is cleared after OnCollected is called to minimise memory usage
	objectStateMap map[string]*TimeRangeCollectionStateImpl

	// TODO do we need to serialise this - it will always be set by the source - we could just use to validate pattern has not changed??
	granularity time.Duration

	// path to the serialised collection state JSON
	jsonPath     string
	lastSaveTime time.Time

	mut *sync.RWMutex
}

func NewArtifactCollectionStateImpl[T artifact_source_config.ArtifactSourceConfig]() CollectionState[T] {
	return &ArtifactCollectionStateImpl[T]{
		TrunkStates:    make(map[string]*TimeRangeCollectionStateImpl),
		objectStateMap: make(map[string]*TimeRangeCollectionStateImpl),
		mut:            &sync.RWMutex{},
	}
}

// Init sets the filepath of the collection state and loads the state from the file if it exists
func (s *ArtifactCollectionStateImpl[T]) Init(_ T, path string) error {
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

// SetGranularity sets the granularity of the collection state - this is determined by the file layout and the
// granularity of the time metadata it contains
func (s *ArtifactCollectionStateImpl[T]) SetGranularity(granularity time.Duration) {
	// ensure the granularity is no smaller than the minimum
	if granularity < MinArtifactGranularity && granularity != 0 {
		granularity = MinArtifactGranularity
	}
	s.granularity = granularity
}

// GetGranularity returns the granularity of the collection state
func (s *ArtifactCollectionStateImpl[T]) GetGranularity() time.Duration {
	return s.granularity
}

func (s *ArtifactCollectionStateImpl[T]) GetStartTime() time.Time {
	// find the latest start time of all the trunk states
	var startTime time.Time
	for _, trunkState := range s.TrunkStates {
		if trunkState == nil {
			continue
		}
		s := trunkState.GetStartTime()
		if s.IsZero() {
			continue
		}
		if startTime.IsZero() || s.After(startTime) {
			startTime = s
		}
	}
	return startTime
}

func (s *ArtifactCollectionStateImpl[T]) GetEndTime() time.Time {
	// find the earliest end time of all the trunk states
	var endTime time.Time
	for _, trunkState := range s.TrunkStates {
		if trunkState == nil {
			continue
		}
		if trunkState.GetEndTime().IsZero() {
			continue
		}
		if endTime.IsZero() || trunkState.GetEndTime().Before(endTime) {
			endTime = trunkState.GetEndTime()
		}
	}
	// if there is NO end time, the end of the last collection
	if endTime.IsZero() {
		endTime = s.LastModifiedTime
	}
	return endTime
}

// SetEndTime sets the end time for the collection state - update all trunk states
// This is called when we are using the --from flag to force recollection
func (s *ArtifactCollectionStateImpl[T]) SetEndTime(newEndTime time.Time) {
	if s.granularity == 0 {
		if newEndTime.Before(s.LastModifiedTime) {
			s.Clear()
		}
		return
	}

	// do not lock mut until AFTER we have checked the granularity as Clear also locks mut
	s.mut.Lock()
	defer s.mut.Unlock()

	// call set end time for each trunk state
	for _, s := range s.TrunkStates {
		s.SetEndTime(newEndTime)
	}
}

func (s *ArtifactCollectionStateImpl[T]) Clear() {
	s.mut.Lock()
	defer s.mut.Unlock()
	// cleat the map
	s.TrunkStates = make(map[string]*TimeRangeCollectionStateImpl)
}

// RegisterPath registers a path with the collection state - we determine whether this is a potential trunk
// (i.e. a path segment with no time metadata for which we need to track collection state separately)
// and if so, add it to the map of trunk states
func (s *ArtifactCollectionStateImpl[T]) RegisterPath(path string, metadata map[string]string) {
	// if this a trunk (i.e. there is no time component)
	// if so, add an entry in the trunk states map
	if s.containsTimeMetadata(metadata) {
		return
	}

	// do we already have a trunk that covers this path?
	var trunksToDelete []string
	for t := range s.TrunkStates {
		// if an existing trunk has this path as a prefix, we have nothing to do
		if strings.HasPrefix(t, path) {
			return
		}
		// if this path is a prefix of an existing trunk, we should delete the existing trunk
		if strings.HasPrefix(path, t) {
			trunksToDelete = append(trunksToDelete, t)
		}
	}
	// delete the shorter trunks
	for _, t := range trunksToDelete {
		delete(s.TrunkStates, t)
	}

	// so there is no time metadata, this is a (potential) trunk
	// add the path to the trunk states
	if _, ok := s.TrunkStates[path]; !ok {
		// add nil for now as a placeholder - we will instantiate when/if we find a file in this folder
		s.TrunkStates[path] = nil
	}
}

// ShouldCollect returns whether the object should be collected, based on the time metadata in the object
func (s *ArtifactCollectionStateImpl[T]) ShouldCollect(id string, timestamp time.Time) bool {
	s.mut.Lock()
	defer s.mut.Unlock()

	// find the trunk state for this object
	itemPath := id

	// find all matching trunks and choose the longest
	var trunkPath string
	var collectionState *TimeRangeCollectionStateImpl

	for t, trunkState := range s.TrunkStates {
		if strings.HasPrefix(itemPath, t) && len(t) > len(trunkPath) {
			trunkPath = t
			collectionState = trunkState
		}
	}

	// we should always have a trunk state
	if len(trunkPath) == 0 {
		slog.Error("No trunk state found for item - not collectiong", "item", itemPath)
		return false
	}
	if collectionState == nil {
		// create a new collection state for this trunk
		collectionState = NewTimeRangeCollectionStateImpl(CollectionOrderChronological)
		// set the granularity
		collectionState.SetGranularity(s.granularity)

		// write it back
		s.TrunkStates[trunkPath] = collectionState
	}

	// ask the collection state if we should collect this object
	res := collectionState.ShouldCollect(id, timestamp)

	// now we have figured out which collection state to use, store that mapping for use in OnCollected
	// - we need to know which collection state to update when we collect the object
	if res {
		s.objectStateMap[itemPath] = collectionState
	}
	return res
}

// OnCollected is called when an object has been collected - update our end time and end objects if needed
func (s *ArtifactCollectionStateImpl[T]) OnCollected(id string, timestamp time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// store modified time to ensure we save the state
	s.LastModifiedTime = time.Now()

	// we should have stored a collection state mapping for this object
	collectionState, ok := s.objectStateMap[id]
	if !ok {
		return fmt.Errorf("no collection state mapping found for item '%s' - this should have been set in ShouldCollect", id)
	}
	// clear the mapping
	delete(s.objectStateMap, id)

	return collectionState.OnCollected(id, timestamp)
}

// Save serialises the collection state to a JSON file
func (s *ArtifactCollectionStateImpl[T]) Save() error {
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
	err = os.WriteFile(s.jsonPath, jsonBytes, 0644)
	if err != nil {
		return fmt.Errorf("failed to write collection state to file: %w", err)
	}

	// update the last save time
	s.lastSaveTime = time.Now()

	return nil
}

// IsEmpty returns whether the collection state is empty
func (s *ArtifactCollectionStateImpl[T]) IsEmpty() bool {
	for _, trunkState := range s.TrunkStates {
		if trunkState != nil && !trunkState.IsEmpty() {
			return false
		}
	}
	return true
}

// helper to determine if the metadata contains any time metadata
func (s *ArtifactCollectionStateImpl[T]) containsTimeMetadata(metadata map[string]string) bool {
	// check for any time metadata
	timeFields := []string{
		constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay, constants.TemplateFieldHour, constants.TemplateFieldMinute, constants.TemplateFieldSecond,
	}
	for _, f := range timeFields {
		if _, ok := metadata[f]; ok {
			return true
		}
	}
	return false
}
