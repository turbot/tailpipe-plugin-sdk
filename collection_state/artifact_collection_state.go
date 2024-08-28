package collection_state

import (
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
	"regexp"
	"time"
)

// ArtifactCollectionState is the interface for the collection state of an S3 bucket
// return the start time and the end time for the data downloaded

type ArtifactMetadata struct {
	Timestamp time.Time `json:"timestamp,omitempty"`
	// TODO add size
}

func newArtifactMetadata(metadata *types.ArtifactInfo) *ArtifactMetadata {
	return &ArtifactMetadata{Timestamp: metadata.Timestamp}
}

type ArtifactCollectionState struct {
	CollectionStateBase
	// the time rage of the data in the bucket
	StartTime time.Time `json:"start_time,omitempty"`
	EndTime   time.Time `json:"end_time,omitempty"`

	// for the start and end boundary we store the metadata
	// we need 2 maps as each time the start or end time changes, we must clear the corresponding map
	StartObjects map[string]*ArtifactMetadata `json:"start_objects,omitempty"`
	EndObjects   map[string]*ArtifactMetadata `json:"end_objects,omitempty"`

	re *regexp.Regexp
	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	granularity time.Duration
}

func NewArtifactCollectionState(opts ...CollectStateOption) CollectionState {
	// NOTE: no need to create maps here - they are created when needed
	res := &ArtifactCollectionState{}
	for _, opt := range opts {
		opt(res)
	}
	return res
}

func (s *ArtifactCollectionState) Init(fileLayout *string) error {
	slog.Info(fmt.Sprintf("Initializing ArtifactCollectionState %p", s), "fileLayout", fileLayout)
	// if we do not have a file layout, we have nothing to do
	if fileLayout == nil {
		// nothing more to do
		return nil
	}

	// convert pattern to a regex
	re, err := regexp.Compile(*fileLayout)
	if err != nil {
		return err
	}
	s.re = re

	// deduce granularity from the regex
	s.getGranularityFromRegex()

	return nil
}

// ShouldCollect returns whether the object should be collected
func (s *ArtifactCollectionState) ShouldCollect(m *types.ArtifactInfo) bool {
	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we must collect everything
	if s.granularity == 0 {
		// if we do not have a granularity we only use the start map
		return !s.inMap(m, s.StartObjects)
	}

	// validate that this artifact name contains the required fields for the current granularity
	if err := s.validateGranularity(m); err != nil {
		// TODO #error should this be an error instead
		// we cannot accuratley determine the time for this artifact - collect it anyway
		return true
	}

	// if the time is between the start and end time we should NOT collect (as have already collected it
	// - assuming consistent artifact ordering)
	if m.Timestamp.After(s.StartTime) && m.Timestamp.Before(s.EndTime) {
		return false
	}

	// if the timer is identical to start or end time, we must check if we have already collected it
	// (as we have reached the limit of the granularity)
	if m.Timestamp.Equal(s.StartTime) {
		return !s.inMap(m, s.StartObjects)
	}
	if m.Timestamp.Equal(s.EndTime) {
		return !s.inMap(m, s.EndObjects)
	}

	// so it before the current start time or after the current end time - we should collect
	return true
}

// Upsert adds new/updates an existing object with its current metadata
// Note: the object name is the full path to the object
func (s *ArtifactCollectionState) Upsert(metadata *types.ArtifactInfo) {
	s.Mut.Lock()
	defer s.Mut.Unlock()

	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we must collect everything
	if s.granularity == 0 {
		s.StartObjects[metadata.Name] = newArtifactMetadata(metadata)
		return
	}

	// update our start and end times as needed
	if metadata.Timestamp.Before(s.StartTime) || s.StartTime.IsZero() {
		// store new start time
		s.StartTime = metadata.Timestamp
		// clear the start objects
		s.StartObjects = make(map[string]*ArtifactMetadata)
	}
	if metadata.Timestamp.After(s.EndTime) || s.EndTime.IsZero() {
		// store new end time
		s.EndTime = metadata.Timestamp
		// clear the end objects
		s.EndObjects = make(map[string]*ArtifactMetadata)
	}

	// if the time equals the start or end time, store the object metadata
	if metadata.Timestamp.Equal(s.StartTime) {
		s.StartObjects[metadata.Name] = newArtifactMetadata(metadata)
	}
	if metadata.Timestamp.Equal(s.EndTime) {
		s.EndObjects[metadata.Name] = newArtifactMetadata(metadata)
	}
}

// the 'granularity' means what it the shortest period we can determine that an artifact comes from based on its filename
// e.g., if the filename contains {year}/{month}/{day}/{hour}/{minute}, the granularity is 1 minute
// if the filename contains {year}/{month}/{day}/{hour}, the granularity is 1 hour
// NOTE: we traverse the time properties from largest to smallest
func (s *ArtifactCollectionState) getGranularityFromRegex() {
	// get the named capture groups from the regex
	captureGroups := s.re.SubexpNames()
	propertyLookup := utils.SliceToLookup(captureGroups)

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
	slog.Info("getGranularityFromRegex", "granularity", s.granularity, "capture groups", captureGroups)
}

// ArtifactCollectionStateOption is a function that sets an option on the ArtifactCollectionState
func (s *ArtifactCollectionState) validateGranularity(a *types.ArtifactInfo) interface{} {
	if s.granularity == 0 {
		// no granularity set, so we are collecting everything
		return nil
	}
	var expectedKeys []string

	switch {

	case s.granularity < time.Minute:
		// granularity < min - we expect year, month, day, hour, minute, second
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay, constants.TemplateFieldHour, constants.TemplateFieldMinute, constants.TemplateFieldSecond}
	case s.granularity < time.Hour:
		// granularity < hour - we expect year, month, day, hour, minute
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay, constants.TemplateFieldHour, constants.TemplateFieldMinute}
	case s.granularity < time.Hour*24:
		// granularity < day - we expect year, month, day, hour
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay, constants.TemplateFieldHour}
	case s.granularity < time.Hour*24*30:
		// granularity < month - we expect year, month, day
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay}
	case s.granularity < time.Hour*24*365:
		// granularity < year - we expect year, month
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth}
	default:
		// granularity >= year - we expect year
		expectedKeys = []string{constants.TemplateFieldYear}
	}

	//	 now verify that we have all the expected keys - check the original properties
	originalProperties := a.GetOriginalProperties()
	for _, key := range expectedKeys {
		if _, ok := originalProperties[key]; !ok {
			slog.Warn("validateGranularity: missing key", "granularity", s.granularity, "key", key)
			return fmt.Errorf("missing key %s", key)
		}
	}
	return nil

}

func (s *ArtifactCollectionState) ParseFilename(fileName string) (_ map[string]string, err error) {
	result := make(map[string]string)
	// Match the filename against the re
	match := s.re.FindStringSubmatch(fileName)
	if match != nil {
		// Create a map to store the extracted values
		for i, name := range s.re.SubexpNames() {
			if i != 0 && name != "" {
				result[name] = match[i]
			}
		}
	}

	return result, nil
}

func (s *ArtifactCollectionState) inMap(m *types.ArtifactInfo, objectMap map[string]*ArtifactMetadata) bool {
	s.Mut.RLock()
	defer s.Mut.RUnlock()

	_, ok := objectMap[m.Name]
	return ok
}

func (s *ArtifactCollectionState) Initialized() bool {
	return s.re != nil
}
