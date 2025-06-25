package collection_state

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

const MinArtifactGranularity = time.Hour * 24

// ArtifactCollectionState is a collection state implementation for artifact sources
// it tracks the collection state for each trunk (a path segment that does not contain any time metadata)
// NOTE: in use, this will be wrapped in a SaveableCollectionState to allow saving to disk.
// This also implements locking for OnCollected and ShouldCollect methods so we do not need to do that here.
type ArtifactCollectionState struct {
	// map of trunk paths to collection state for that trunk
	// a trunk is a path segment that does not contain any time metadata
	// for example if the path is s3://bucket/folder1/folder2/2021/01/01/file.txt then the trunk is s3://bucket/folder1/folder2
	TrunkStates map[string]*TimeRangeCollectionState `json:"trunk_states,omitempty"`

	granularity time.Duration

	// the time range for the underway collection - populated by Init
	currentCollectionTimeRange *CollectionTimeRange
	// map of the trunk state for each object which has been passed to ShouldCollect
	// this is to avoid recomputing the trunk state for each object on every OnCollected call
	objectTrunkMap map[string]*TimeRangeCollectionState
}

func NewArtifactCollectionState() CollectionState {
	return &ArtifactCollectionState{
		TrunkStates:    make(map[string]*TimeRangeCollectionState),
		objectTrunkMap: make(map[string]*TimeRangeCollectionState),
	}
}

// Init sets the filepath of the collection state and loads the state from the file if it exists
func (s *ArtifactCollectionState) Init(collectionTimeRange CollectionTimeRange, granularity time.Duration) {
	// ensure the granularity is no smaller than the minimum
	if granularity < MinArtifactGranularity && granularity != 0 {
		granularity = MinArtifactGranularity
	}
	s.granularity = granularity

	s.currentCollectionTimeRange = &collectionTimeRange

	// call init on all trunk states to ensure they are initialised
	for _, trunkState := range s.TrunkStates {
		if trunkState != nil {
			trunkState.Init(collectionTimeRange, granularity)
		}
	}
}

func (s *ArtifactCollectionState) GetFromTime() time.Time {
	// find the latest start time of all the trunk states
	var startTime time.Time
	for _, trunkState := range s.TrunkStates {
		if trunkState == nil {
			continue
		}
		s := trunkState.GetFromTime()
		if s.IsZero() {
			continue
		}
		if startTime.IsZero() || s.After(startTime) {
			startTime = s
		}
	}
	return startTime
}

// GetToTime returns the time we know have collected ALL data up until
// (we may have collected some data after this - within the granularity period)
// return the earliest end time of all the trunk states
func (s *ArtifactCollectionState) GetToTime() time.Time {
	// TODO #CS KAI think about continuation for reverse order
	// find the earliest end time of all the trunk states
	var endTime time.Time
	for _, trunkState := range s.TrunkStates {
		if trunkState == nil {
			continue
		}
		if trunkState.GetToTime().IsZero() {
			continue
		}
		if endTime.IsZero() || trunkState.GetToTime().Before(endTime) {
			endTime = trunkState.GetToTime()
		}
	}

	return endTime
}

// RegisterPath registers a path with the collection state - we determine whether this is a potential trunk
// (i.e. a path segment with no time metadata for which we need to track collection state separately)
// and if so, add it to the map of trunk states
func (s *ArtifactCollectionState) RegisterPath(path string, metadata map[string]string) {
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
func (s *ArtifactCollectionState) ShouldCollect(id string, timestamp time.Time) bool {
	if s.currentCollectionTimeRange == nil {
		slog.Error("ShouldCollect called before Init", "id", id, "timestamp", timestamp)
		return false
	}

	trunkState := s.getTrunkState(id)

	// cache the trunk for this object
	s.objectTrunkMap[id] = trunkState

	return trunkState.ShouldCollect(id, timestamp)
}

func (s *ArtifactCollectionState) getTrunkState(id string) *TimeRangeCollectionState {
	rootChar := "/"
	var trunkPath string

	// find the trunk state for this object
	itemPath := id

	// find all matching trunks and choose the longest
	for t := range s.TrunkStates {
		if strings.HasPrefix(itemPath, t) && len(t) > len(trunkPath) {
			trunkPath = t
		}
	}

	// we should always have a trunk state
	if len(trunkPath) == 0 {
		trunkPath = rootChar
	}

	// now we have a trunk, find which time range collection state to use
	trunkState, ok := s.TrunkStates[trunkPath]
	// if we have a trunk state, get the range for this timestamp (this will create a new range if needed)
	if !ok || trunkState == nil {
		// so we DO NOT have a collection state for this trunk
		// create a new collection state
		trunkState = NewTimeRangeCollectionState().(*TimeRangeCollectionState)
		// initialize the trunk state with the current collection time range
		trunkState.Init(*s.currentCollectionTimeRange, s.granularity)

		// write the state back to TrunkStates
		s.TrunkStates[trunkPath] = trunkState
	}
	return trunkState
}

// OnCollected is called when an object has been collected - update our end time and end objects if needed
func (s *ArtifactCollectionState) OnCollected(id string, timestamp time.Time) error {

	// we should have a trunk cached for this object
	trunkState, ok := s.objectTrunkMap[id]
	if !ok {
		return fmt.Errorf("no trunk mapping found for item '%s' - this should have been set in ShouldCollect", id)
	}
	// remove the trunk mapping for this object
	delete(s.objectTrunkMap, id)

	return trunkState.OnCollected(id, timestamp)

}

// OnCollectionComplete sets the end time for the collection state - update all trunk states
// This is called after a successful collection to set the collection state end time to the to time of the collection
func (s *ArtifactCollectionState) OnCollectionComplete() error {
	for _, trunkState := range s.TrunkStates {
		if trunkState == nil {
			continue
		}
		// set the end time of the trunk state to the end time of the current collection
		err := trunkState.OnCollectionComplete()
		if err != nil {
			return err
		}
	}
	return nil
}

// IsEmpty returns whether the collection state is empty
func (s *ArtifactCollectionState) IsEmpty() bool {
	for _, trunkState := range s.TrunkStates {
		if trunkState != nil && !trunkState.IsEmpty() {
			return false
		}
	}
	return true
}

func (s *ArtifactCollectionState) Clear(timeRange CollectionTimeRange) {
	for _, trunkState := range s.TrunkStates {
		if trunkState == nil {
			continue
		}
		// clear the trunk state for the current collection time range
		trunkState.Clear(timeRange)
	}

	slog.Debug("Collection state after clearing", "state", s.String())
}

// MigrateFromLegacyState attempts to migrate from a legacy collection state
func (s *ArtifactCollectionState) MigrateFromLegacyState(bytes []byte) error {
	legacyState := &ArtifactCollectionStateLegacy{}
	err := json.Unmarshal(bytes, legacyState)
	if err != nil {
		return fmt.Errorf("failed to unmarshal legacy collection state: %w", err)
	}

	// Convert each trunk state from legacy format to new format
	for trunkPath, legacyTrunkState := range legacyState.TrunkStates {
		if legacyTrunkState == nil {
			// Skip nil trunk states
			continue
		}

		// Use the new constructor for legacy trunk states
		s.TrunkStates[trunkPath] = NewTimeRangeCollectionStateFromLegacy(legacyTrunkState)
	}

	return nil
}

func (s *ArtifactCollectionState) Validate() error {
	var errorList []error
	for _, trunkState := range s.TrunkStates {
		if trunkErr := trunkState.Validate(); trunkErr != nil {
			errorList = append(errorList, trunkErr)
		}
	}
	if len(errorList) > 0 {
		return fmt.Errorf("validation failed for artifact collection state: %w", errors.Join(errorList...))
	}
	return nil
}

// Compare compares the current state with another ArtifactCollectionState and returns whether they are equal
// and a message describing any differences
func (s *ArtifactCollectionState) Compare(want *ArtifactCollectionState) (bool, string) {
	if len(s.TrunkStates) != len(want.TrunkStates) {
		return false, fmt.Sprintf("trunk count = %v, want %v", len(s.TrunkStates), len(want.TrunkStates))
	}
	for k, expectedTrunk := range want.TrunkStates {
		actualTrunk, ok := s.TrunkStates[k]
		if !ok {
			return false, fmt.Sprintf("missing trunk %v", k)
		}
		if equal, msg := actualTrunk.Compare(expectedTrunk); !equal {
			return false, fmt.Sprintf("trunk %v: %s", k, msg)
		}
	}
	if s.granularity != want.granularity {
		return false, fmt.Sprintf("granularity = %v, want %v", s.granularity, want.granularity)
	}
	return true, ""
}

// helper to determine if the metadata contains any time metadata
func (s *ArtifactCollectionState) containsTimeMetadata(metadata map[string]string) bool {
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

func (s *ArtifactCollectionState) String() any {
	stringBuilder := strings.Builder{}
	stringBuilder.WriteString("ArtifactCollectionState:\n")
	for trunkPath, trunkState := range s.TrunkStates {
		if trunkState == nil {
			stringBuilder.WriteString(fmt.Sprintf("  %s: <nil>\n", trunkPath))
			continue
		}
		stringBuilder.WriteString(fmt.Sprintf("  %s: %s\n", trunkPath, trunkState.String()))
	}
	return stringBuilder.String()
}
