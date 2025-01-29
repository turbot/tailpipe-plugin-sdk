package helpers

import (
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"golang.org/x/exp/maps"
	"log/slog"
	"regexp"
	"time"
)

// ExtractNamedGroupsFromGrok extracts named groups from a Grok pattern
func ExtractNamedGroupsFromGrok(grokPattern string) []string {
	// Regular expression to match named Grok patterns (e.g., %{WORD:org})
	re := regexp.MustCompile(`%{\w+:(\w+)}`)
	matches := re.FindAllStringSubmatch(grokPattern, -1)

	// Collect all group names
	var groupNames []string
	for _, match := range matches {
		if len(match) > 1 {
			groupNames = append(groupNames, match[1])
		}
	}
	return groupNames
}

// GetGranularityFromFileLayout is a helper function to determine the granularity of the collection state based on the file layout
//
// the 'granularity' means what it the shortest period we can determine that an artifact comes from based on its filename
// e.g., if the filename contains {year}/{month}/{day}/{hour}/{minute}, the granularity is 1 minute
// if the filename contains {year}/{month}/{day}/{hour}, the granularity is 1 hour
// NOTE: we traverse the time properties from largest to smallest
func GetGranularityFromFileLayout(fileLayout *string) time.Duration {
	if fileLayout == nil {
		return 0
	}

	// get the named capture groups from the regex
	captureGroups := ExtractNamedGroupsFromGrok(*fileLayout)
	propertyLookup := utils.SliceToLookup(captureGroups)

	slog.Info("GetGranularityFromFileLayout", "capture groups", captureGroups, "keys", maps.Keys(propertyLookup))

	// check year/month/day/hour/minute/second
	if _, ok := propertyLookup[constants.TemplateFieldYear]; ok {
		if _, ok := propertyLookup[constants.TemplateFieldMonth]; ok {
			if _, ok := propertyLookup[constants.TemplateFieldDay]; ok {
				if _, ok := propertyLookup[constants.TemplateFieldHour]; ok {
					if _, ok := propertyLookup[constants.TemplateFieldMinute]; ok {
						if _, ok := propertyLookup[constants.TemplateFieldSecond]; ok {
							return time.Second
						}
						return time.Minute
					}
					return time.Hour
				}
				return time.Hour * 24
			}
			return time.Hour * 24 * 30
		}
		return time.Hour * 24 * 365
	}

	return 0
}
