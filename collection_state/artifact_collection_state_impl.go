package collection_state

import (
	"fmt"
	"golang.org/x/exp/maps"
	"log/slog"
	"time"

	"github.com/elastic/go-grok"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// ArtifactCollectionStateImpl is the interface for the collection state of an S3 bucket
// return the start time and the end time for the data downloaded

type ArtifactMetadata struct {
	Timestamp time.Time `json:"timestamp,omitempty"`
	// TODO add size
}

func newArtifactMetadata(info *types.ArtifactInfo) *ArtifactMetadata {
	return &ArtifactMetadata{Timestamp: info.Timestamp}
}

type ArtifactCollectionStateImpl[T artifact_source_config.ArtifactSourceConfig] struct {
	CollectionStateBase

	// for  end boundary we store the metadata
	// each time the end time changes, we must clear the map
	EndObjects map[string]*ArtifactMetadata `json:"end_objects,omitempty"`

	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	granularity time.Duration
	// the grok parser
	g *grok.Grok
}

func NewArtifactCollectionStateImpl[T artifact_source_config.ArtifactSourceConfig]() CollectionState[T] {
	// NOTE: no need to create maps here - they are created when needed
	return &ArtifactCollectionStateImpl[T]{}
}

func (s *ArtifactCollectionStateImpl[T]) Init(config T) error {
	fileLayout := config.GetFileLayout()
	slog.Info(fmt.Sprintf("Initializing ArtifactCollectionStateImpl %p", s), "fileLayout", fileLayout)
	// create a grok parser even if we do not have a file layout - we use it to check for initalized state
	g := grok.New()

	// if we do not have a file layout, we have nothing to do
	if fileLayout == nil {
		// nothing more to do
		return nil
	}

	// convert pattern to a grok parser
	err := g.Compile(*fileLayout, true)
	if err != nil {
		return err
	}

	// deduce granularity from the regex
	s.getGranularityFromMetadata(*fileLayout)

	return nil
}

// ShouldCollect returns whether the object should be collected
func (s *ArtifactCollectionStateImpl[T]) ShouldCollect(m *types.ArtifactInfo) bool {
	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we use start objects to track everythinbg
	if s.granularity == 0 {
		// if we do not have a granularity we only use the start map
		return !s.inMap(m, s.EndObjects)
	}

	// if the time is between the start and end time we should NOT collect (as have already collected it
	// - assuming consistent artifact ordering)
	if m.Timestamp.After(s.StartTime) && m.Timestamp.Before(s.EndTime) {
		return false
	}

	// if the timer is <= the end time + granularity, we must check if we have already collected it
	// (as we have reached the limit of the granularity)
	if m.Timestamp.Compare(s.EndTime.Add(s.granularity)) <= 0 {
		return !s.inMap(m, s.EndObjects)
	}

	// so it before the current start time or after the current end time - we should collect
	return true
}

func (s *ArtifactCollectionStateImpl[T]) GetGranularity() time.Duration {
	return s.granularity
}

// OnCollected is called when an object has been collected - update the end time and end objects if needed
// Note: the object name is the full path to the object
func (s *ArtifactCollectionStateImpl[T]) OnCollected(metadata *types.ArtifactInfo) error {
	s.Mut.Lock()
	defer s.Mut.Unlock()

	// if start time i snot set, set it now
	if s.StartTime.IsZero() {
		s.StartTime = metadata.Timestamp
	}

	// if this timestamp is BEFORE the start time, data is being collected out of order - we do not (currently) support this
	if metadata.Timestamp.Before(s.StartTime) {
		slog.Warn("OnCollected: timestamp before start time", "timestamp", metadata.Timestamp, "start time", s.StartTime)
		return fmt.Errorf("artifact timestamp before collection state start time")
	}
	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we must collect everything
	if s.granularity == 0 {
		s.EndObjects[metadata.LocalName] = newArtifactMetadata(metadata)
		return nil
	}

	// update our end times as neede
	// NOTE: the end time are adjusted by the granularity
	// i.e if the granularity is 1 hour, and the artifact time is 12:00:00, the end time will be 11:00:00,
	// i.e. we are sure we have collected ALL data up to 11:59:59
	endTime := metadata.Timestamp.Add(-s.granularity)
	if endTime.After(s.EndTime) || s.EndTime.IsZero() {
		s.SetEndTime(endTime)
	}

	//TODO THINK ABOUT THIS
	// if the time equals the start or end time, store the object metadata
	if metadata.Timestamp.Equal(s.EndTime) {
		s.EndObjects[metadata.LocalName] = newArtifactMetadata(metadata)
	}
	return nil
}

func (s *ArtifactCollectionStateImpl[T]) IsEmpty() bool {
	return s.StartTime.IsZero()
}

// SetStartTime overrides the base implementation to also clear the start objects
//func (s *ArtifactCollectionStateImpl[T]) SetStartTime(t time.Time) {
//	s.StartTime = t
//	s.StartObjects = make(map[string]*ArtifactMetadata)
//}

// SetEndTime overrides the base implementation to also clear the end objects
func (s *ArtifactCollectionStateImpl[T]) SetEndTime(t time.Time) {
	s.EndTime = t
	s.EndObjects = make(map[string]*ArtifactMetadata)
}

// the 'granularity' means what it the shortest period we can determine that an artifact comes from based on its filename
// e.g., if the filename contains {year}/{month}/{day}/{hour}/{minute}, the granularity is 1 minute
// if the filename contains {year}/{month}/{day}/{hour}, the granularity is 1 hour
// NOTE: we traverse the time properties from largest to smallest
func (s *ArtifactCollectionStateImpl[T]) getGranularityFromMetadata(fileLayout string) {

	// get the named capture groups from the regex
	captureGroups := helpers.ExtractNamedGroupsFromGrok(fileLayout)
	propertyLookup := utils.SliceToLookup(captureGroups)

	slog.Info("getGranularityFromMetadata", "capture groups", captureGroups, "keys", maps.Keys(propertyLookup))
	// check year/month/day/hour/minute/second
	if _, ok := propertyLookup[constants.TemplateFieldYear]; ok {
		if _, ok := propertyLookup[constants.TemplateFieldMonth]; ok {
			if _, ok := propertyLookup[constants.TemplateFieldDay]; ok {
				if _, ok := propertyLookup[constants.TemplateFieldHour]; ok {
					if _, ok := propertyLookup[constants.TemplateFieldMinute]; ok {
						if _, ok := propertyLookup[constants.TemplateFieldSecond]; ok {
							s.granularity = time.Second
							return
						}
						s.granularity = time.Minute
						return
					}
					s.granularity = time.Hour
					return
				}
				s.granularity = time.Hour * 24
				return
			}
			s.granularity = time.Hour * 24 * 30
			return
		}
		s.granularity = time.Hour * 24 * 365
	}

	//	 nothing found, leave granularity as 0
	slog.Info("getGranularityFromMetadata", "granularity", s.granularity, "capture groups", captureGroups)
}

func (s *ArtifactCollectionStateImpl[T]) inMap(m *types.ArtifactInfo, objectMap map[string]*ArtifactMetadata) bool {
	s.Mut.RLock()
	defer s.Mut.RUnlock()

	_, ok := objectMap[m.LocalName]
	return ok
}

func (s *ArtifactCollectionStateImpl[T]) Initialized() bool {
	return s.g != nil
}
