package helpers

import "regexp"

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
