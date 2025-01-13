package collection_state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-grok"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

// ArtifactCollectionStateImpl is the interface for the collection state of an S3 bucket
// return the start time and the end time for the data downloaded

type ArtifactCollectionStateImpl[T artifact_source_config.ArtifactSourceConfig] struct {
	mut sync.RWMutex

	// map of trunk paths to collection state for that trunk
	// TODO comment to define trunk
	trunkStates map[string]*CollectionStateImpl[T]
	// map of object identifier to collection state which contains the object
	// used to store the collection state for each object between the ShouldCollect call and the OnCollected call
	// NOTE: the map entry is cleared after OnCollected is called to minimise memory usage
	objectStateMap map[string]*CollectionStateImpl[T]

	granularity time.Duration

	// path to the serialised collection state JSON
	jsonPath         string
	lastModifiedTime time.Time
	lastSaveTime     time.Time

	// the grok parser
	g *grok.Grok
}

func NewArtifactCollectionStateImpl[T artifact_source_config.ArtifactSourceConfig]() CollectionState[T] {
	return &ArtifactCollectionStateImpl[T]{
		trunkStates:    make(map[string]*CollectionStateImpl[T]),
		objectStateMap: make(map[string]*CollectionStateImpl[T]),
	}
}

func (s *ArtifactCollectionStateImpl[T]) RegisterPath(path string, metadata map[string]string) {
	// if this a trunk (i.e. there is no time component)
	// if so, add an entry in the trunk states map
	if s.containsTimeMetadata(metadata) {
		return
	}

	// so there is no time metadata, this is a (potential) trunk
	// add the path to the trunk states
	if _, ok := s.trunkStates[path]; !ok {
		// add nil for now as a placeholder - we will instantiate when/if we find a file in this folder
		s.trunkStates[path] = nil
	}
}

func (s *ArtifactCollectionStateImpl[T]) Init(_ T, path string) error {
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

func (s *ArtifactCollectionStateImpl[T]) SetGranularity(granularity time.Duration) {
	s.granularity = granularity
}

// ShouldCollect returns whether the object should be collected
// TODO return error as well
func (s *ArtifactCollectionStateImpl[T]) ShouldCollect(m SourceItemMetadata) bool {
	s.mut.Lock()
	defer s.mut.Unlock()

	// find the trunk state for this object
	itemPath := m.Identifier()

	// find all matching trunks and choose the longest
	var trunkPath string
	var collectionState *CollectionStateImpl[T]
	var trunksToDelete []string
	for t, trunkState := range s.trunkStates {
		if strings.HasPrefix(itemPath, t) && len(t) > len(trunkPath) {
			if len(trunkPath) > 0 {
				// we have found a longer trunk, so we should delete the shorter trunk
				trunksToDelete = append(trunksToDelete, trunkPath)
			}

			trunkPath = t
			collectionState = trunkState
		}
	}
	// delete the shorter trunks
	for _, t := range trunksToDelete {
		delete(s.trunkStates, t)
	}

	// we should always have a trunk state
	if len(trunkPath) == 0 {
		// TODO return error as well
		slog.Error("No trunk state found for item", "item", itemPath)
		return false
	}
	if collectionState == nil {
		// create a new collection state for this trunk
		// TODO split up CollectionStateImpl to to TimeRange and Base - just use timerange here
		collectionState = &CollectionStateImpl[T]{}
		s.trunkStates[trunkPath] = collectionState
	}

	// ask the collection state if we should collect this object
	res := collectionState.ShouldCollect(m)

	// now we have figured out which collection state to use, store that mapping for use in OnCollected
	if res {
		s.objectStateMap[itemPath] = collectionState
	}
	return res
}

func (s *ArtifactCollectionStateImpl[T]) OnCollected(metadata SourceItemMetadata) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// we should have stored a collection state mapping for this object
	collectionState, ok := s.objectStateMap[metadata.Identifier()]
	if !ok {
		return fmt.Errorf("no collection state mapping found for item - this should have been set in ShouldCollect", "item", metadata.Identifier())
	}
	// clear the mapping
	delete(s.objectStateMap, metadata.Identifier())

	return collectionState.OnCollected(metadata)
}

func (s *ArtifactCollectionStateImpl[T]) Save() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// if the last save time is after the last modified time, then we have nothing to do
	if s.lastSaveTime.After(s.lastModifiedTime) {
		slog.Debug("collection state has not been modified since last save")
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

func (s *ArtifactCollectionStateImpl[T]) GetGranularity() time.Duration {
	return s.granularity
}

func (s *ArtifactCollectionStateImpl[T]) IsEmpty() bool {
	for _, trunkState := range s.trunkStates {
		if trunkState != nil {
			return false
		}
	}
	return true
}

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
