package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"strconv"
	"time"
)

func dirSatisfiesFromTime(fromTime time.Time, metadata map[string]string) bool {
	// this is similar to ArtifactInfo.parseArtifactTimestamp except we do not return an error if we have a missing key

	// define a map of values - we will populate values determined by the granularity
	valueLookup := map[string]int{
		constants.TemplateFieldYear:   0,
		constants.TemplateFieldMonth:  0,
		constants.TemplateFieldDay:    0,
		constants.TemplateFieldHour:   0,
		constants.TemplateFieldMinute: 0,
		constants.TemplateFieldSecond: 0,
	}

	// we check each time unit in turn to see if we have metadata for it
	expectedKeys := []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay, constants.TemplateFieldHour, constants.TemplateFieldMinute, constants.TemplateFieldSecond}

	// as we do so, we determine the granularty of the timestamp we build - we use this to truncate the fromTime for comparison
	var granularity = time.Second
	granularities := map[string]time.Duration{
		constants.TemplateFieldYear:   time.Hour * 24 * 365,
		constants.TemplateFieldMonth:  time.Hour * 24 * 30,
		constants.TemplateFieldDay:    time.Hour * 24,
		constants.TemplateFieldHour:   time.Hour,
		constants.TemplateFieldMinute: time.Minute,
		constants.TemplateFieldSecond: time.Second,
	}

	for _, key := range expectedKeys {
		if _, ok := metadata[key]; !ok {
			// if we don't even have a year,  give up
			if key == constants.TemplateFieldYear {
				// return true - this filder does not have date information so it satisfies the from time
				return true
			}

			break
		}
		valString := metadata[key]
		val, err := strconv.Atoi(valString)
		if err != nil {
			return false
		}
		valueLookup[key] = val
		granularity = granularities[key]
	}
	// build timestamp from the properties provided
	timestamp := time.Date(
		valueLookup[constants.TemplateFieldYear],
		time.Month(valueLookup[constants.TemplateFieldMonth]),
		valueLookup[constants.TemplateFieldDay],
		valueLookup[constants.TemplateFieldHour],
		valueLookup[constants.TemplateFieldMinute],
		valueLookup[constants.TemplateFieldSecond],
		0,
		time.UTC)

	// truncate the fromTime to the granularity of the timestamp
	truncatedFromTime := fromTime.Truncate(granularity)
	return timestamp.Compare(truncatedFromTime) >= 0
}
