package types

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"log/slog"
	"strconv"
	"time"
)

type ArtifactInfo struct {
	// this is the original name of the artifact
	Name string `json:"original_name"`

	// enrichment values passed from the source to the collection to include in the enrichment process
	SourceEnrichment *schema.SourceEnrichment `json:"-"`
	Timestamp        time.Time                `json:"timestamp"`
}

func NewArtifactInfo(path string, sourceEnrichment *schema.SourceEnrichment, granularity time.Duration) (*ArtifactInfo, error) {
	res := &ArtifactInfo{
		// original name is the source path of the artifact
		Name:             path,
		SourceEnrichment: sourceEnrichment,
	}
	timeStamp, err := res.parseArtifactTimestamp(granularity)
	if err != nil {
		return nil, err
	}
	res.Timestamp = timeStamp
	return res, nil
}

func ArtifactInfoFromProto(info *proto.ArtifactInfo) *ArtifactInfo {
	enrichment := schema.SourceEnrichmentFromProto(info.SourceEnrichment)
	return &ArtifactInfo{
		Name:             info.OriginalName,
		SourceEnrichment: enrichment,
	}
}

func (a *ArtifactInfo) ToProto() *proto.ArtifactInfo {
	return &proto.ArtifactInfo{
		OriginalName:     a.Name,
		SourceEnrichment: a.SourceEnrichment.ToProto(),
	}
}

// validate the artifact has all properties required to parse the timestamp based on the granularity
// then parse the timestamp and return it
func (a *ArtifactInfo) parseArtifactTimestamp(granularity time.Duration) (time.Time, error) {
	var timestamp time.Time
	if granularity == 0 {
		// TODO IS THIS SUPPORTED? I DON'T THINK SO -> error???
		// no granularity set, so we are collecting everything
		return timestamp, nil
	}
	var expectedKeys []string

	switch {
	case granularity < time.Minute:
		// granularity < min - we expect year, month, day, hour, minute, second
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay, constants.TemplateFieldHour, constants.TemplateFieldMinute, constants.TemplateFieldSecond}
	case granularity < time.Hour:
		// granularity < hour - we expect year, month, day, hour, minute
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay, constants.TemplateFieldHour, constants.TemplateFieldMinute}
	case granularity < time.Hour*24:
		// granularity < day - we expect year, month, day, hour
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay, constants.TemplateFieldHour}
	case granularity < time.Hour*24*30:
		// granularity < month - we expect year, month, day
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay}
	case granularity < time.Hour*24*365:
		// granularity < year - we expect year, month
		expectedKeys = []string{constants.TemplateFieldYear, constants.TemplateFieldMonth}
	default:
		// granularity >= year - we expect year
		expectedKeys = []string{constants.TemplateFieldYear}
	}

	// define a map of values - we will populate values determined by the granularity
	valueLookup := map[string]int{
		constants.TemplateFieldYear:   0,
		constants.TemplateFieldMonth:  0,
		constants.TemplateFieldDay:    0,
		constants.TemplateFieldHour:   0,
		constants.TemplateFieldMinute: 0,
		constants.TemplateFieldSecond: 0,
	}

	// now verify that we have all the expected keys and parse the value
	for _, key := range expectedKeys {
		if _, ok := a.SourceEnrichment.Metadata[key]; !ok {
			slog.Warn("parseArtifactTimestamp: missing key", "granularity", granularity, "key", key)
			return timestamp, fmt.Errorf("missing key %s", key)
		}
		valString := a.SourceEnrichment.Metadata[key]
		val, err := strconv.Atoi(valString)
		if err != nil {
			return timestamp, fmt.Errorf("error parsing %s from '%s': %v", key, valString, err)
		}
		// populate the value in the map
		valueLookup[key] = val
	}

	// build timestamp from the properties provided
	timestamp = time.Date(
		valueLookup[constants.TemplateFieldYear],
		time.Month(valueLookup[constants.TemplateFieldMonth]),
		valueLookup[constants.TemplateFieldDay],
		valueLookup[constants.TemplateFieldHour],
		valueLookup[constants.TemplateFieldMinute],
		valueLookup[constants.TemplateFieldSecond],
		0,
		time.UTC)

	return timestamp, nil

}

// implement SourceItemMetadata

func (a *ArtifactInfo) GetTimestamp() time.Time {
	return a.Timestamp
}

func (a *ArtifactInfo) Identifier() string {
	return a.Name
}
