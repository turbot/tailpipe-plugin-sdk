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
)

// ArtifactCollectionStateImpl is the interface for the collection state of an S3 bucket
// return the start time and the end time for the data downloaded

type ArtifactCollectionStateImpl[T artifact_source_config.ArtifactSourceConfig] struct {
	CollectionStateImpl[T]

	// the grok parser
	g *grok.Grok
}

func NewArtifactCollectionStateImpl[T artifact_source_config.ArtifactSourceConfig]() CollectionState[T] {
	return &ArtifactCollectionStateImpl[T]{}
}

func (s *ArtifactCollectionStateImpl[T]) Init(config T, path string) error {
	// call base init method
	err := s.CollectionStateImpl.Init(config, path)
	if err != nil {
		return err
	}

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
	err = g.Compile(*fileLayout, true)
	if err != nil {
		return err
	}

	// deduce granularity from the regex
	s.getGranularityFromFileLayout(*fileLayout)

	return nil
}

// the 'granularity' means what it the shortest period we can determine that an artifact comes from based on its filename
// e.g., if the filename contains {year}/{month}/{day}/{hour}/{minute}, the granularity is 1 minute
// if the filename contains {year}/{month}/{day}/{hour}, the granularity is 1 hour
// NOTE: we traverse the time properties from largest to smallest
func (s *ArtifactCollectionStateImpl[T]) getGranularityFromFileLayout(fileLayout string) {

	// get the named capture groups from the regex
	captureGroups := helpers.ExtractNamedGroupsFromGrok(fileLayout)
	propertyLookup := utils.SliceToLookup(captureGroups)

	slog.Info("getGranularityFromFileLayout", "capture groups", captureGroups, "keys", maps.Keys(propertyLookup))
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
	slog.Info("getGranularityFromFileLayout", "granularity", s.granularity, "capture groups", captureGroups)
}
