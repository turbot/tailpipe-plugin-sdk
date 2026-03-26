package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"strconv"
	"time"
)

func dirSatisfiesFromTime(fromTime time.Time, metadata map[string]string) bool {
	// this is similar to ArtifactInfo.parseArtifactTimestamp except we do not return an error if we have a missing key

	// define a map of values - we will populate values determined by the granularity
	valueLookup := newTimeMap()
	fromMap := timeToMap(fromTime)
	truncatedFromMap := newTimeMap()

	// we check each time unit in turn to see if we have metadata for it
	expectedKeys := []string{constants.TemplateFieldYear, constants.TemplateFieldMonth, constants.TemplateFieldDay, constants.TemplateFieldHour, constants.TemplateFieldMinute, constants.TemplateFieldSecond}

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
		truncatedFromMap[key] = fromMap[key]
	}
	// build timestamp from the properties provided
	timestamp := timeFromMap(valueLookup)
	truncatedFromTime := timeFromMap(truncatedFromMap)

	return timestamp.Compare(truncatedFromTime) >= 0
}

type timeMap map[string]int

func newTimeMap() timeMap {
	return timeToMap(time.Time{})
}

func timeToMap(t time.Time) map[string]int {
	return map[string]int{
		constants.TemplateFieldYear:   t.Year(),
		constants.TemplateFieldMonth:  int(t.Month()),
		constants.TemplateFieldDay:    t.Day(),
		constants.TemplateFieldHour:   t.Hour(),
		constants.TemplateFieldMinute: t.Minute(),
		constants.TemplateFieldSecond: t.Second(),
	}
}

func timeFromMap(valueLookup map[string]int) time.Time {
	y := valueLookup[constants.TemplateFieldYear]
	m := time.Month(valueLookup[constants.TemplateFieldMonth])
	d := valueLookup[constants.TemplateFieldDay]
	h := valueLookup[constants.TemplateFieldHour]
	minute := valueLookup[constants.TemplateFieldMinute]
	s := valueLookup[constants.TemplateFieldSecond]
	return time.Date(y, m, d, h, minute, s, 0, time.UTC)
}
