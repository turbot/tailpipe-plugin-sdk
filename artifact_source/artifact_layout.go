package artifact_source

import (
	"github.com/elastic/go-grok"
	"github.com/turbot/pipe-fittings/v2/filter"
	"path/filepath"
	"regexp"
	"strings"
)

func ByteMapToStringMap(m map[string][]byte) map[string]string {
	res := make(map[string]string, len(m))
	for k, v := range m {
		res[k] = string(v)
	}
	return res
}

func ExpandPatternIntoOptionalAlternatives(pattern string) []string {
	var res []string

	// Regular expression to match optional segments (e.g., "(%{WORD:org}/)?")
	re := regexp.MustCompile(`\((.+?)\)\?`)
	// If there are no optional segments, return the pattern as-is
	if !re.MatchString(pattern) {
		return []string{pattern}
	}

	// Recursive function to generate all permutations
	var generate func(string) []string
	generate = func(currentPattern string) []string {
		// Find the first optional segment
		match := re.FindStringSubmatch(currentPattern)
		if match == nil {
			return []string{currentPattern}
		}

		// Extract the optional segment and the full match
		fullMatch := match[0]       // The full "(%{WORD:org}/)?"
		optionalSegment := match[1] // The content inside "(...)"

		// Replace only the first occurrence of the optional segment
		withSegment := strings.Replace(currentPattern, fullMatch, optionalSegment, 1)
		withoutSegment := strings.Replace(currentPattern, fullMatch, "", 1)

		// Recursively generate for the remaining optional segments
		return append(
			generate(withSegment),
			generate(withoutSegment)...,
		)
	}

	// Start generating permutations
	res = generate(pattern)

	return res
}

// metadataSatisfiesFilters checks if a path segment satisfies the given filters,
// based on the file layout, which is a grok pattern
// the grok pattern is assumed to start at the beginning of the path segment
func metadataSatisfiesFilters(metadata map[string]string, filters map[string]*filter.SqlFilter) bool {
	// Validate metadata against filters
	for key, value := range metadata {
		// Check if a filter exists for this key
		// NOTE: we assume each filter is for a single key
		if f, exists := filters[key]; exists {
			// Validate the filter against the extracted value
			if !f.Satisfied(map[string]string{key: value}) {
				return false
			}
		}
	}

	return true
}

// getPathMetadata get the metadata from the given file path, based on the file layout
// returns whether the path matches the layout pattern, and the medata map
func getPathMetadata(targetPath, basePath string, layout string, isDir bool, g *grok.Grok) (bool, map[string]string, error) {
	// remove the base path from the path
	relPath, err := filepath.Rel(basePath, targetPath)
	if err != nil {
		return false, nil, err
	}

	// if this is a directory, we just want to evaluate the pattern segments up to this directory
	// so call getPathSegmentMetadata which trims the pattern to match the path length
	var getMetadataFunc func(g *grok.Grok, pathSegment, fileLayout string) (bool, map[string][]byte, error)
	if isDir {
		getMetadataFunc = getPathSegmentMetadata
	} else {
		getMetadataFunc = getPathLeafMetadata
	}
	match, metadata, err := getMetadataFunc(g, relPath, layout)
	if err != nil {
		return false, nil, err
	}

	// convert the metadata to a string map
	return match, ByteMapToStringMap(metadata), nil
}

// getPathSegmentMetadata extracts metadata from a path segment (i.e. a folder)
// based on the file layout, which is a grok pattern
// the grok pattern is assumed to start at the beginning of the path segment
// - it is trimmed to the length of the path segment
func getPathSegmentMetadata(g *grok.Grok, pathSegment, fileLayout string) (bool, map[string][]byte, error) {
	// Split and truncate the file layout to match the path segment's length
	pathParts := strings.Split(pathSegment, "/")
	layoutParts := strings.Split(fileLayout, "/")
	pathLength := len(pathParts)

	// if the path part is empty or is ".", it must be the first segment, do not try to match
	if pathParts[0] == "" || pathParts[0] == "." {
		// marks as matching
		return true, nil, nil
	}

	// if the path segment is longer than the layout-1, the need to check if the penultimate segment is a wildcard
	// for example "AWSLogs/%{WORD:org}/%{DATA}/%{WORD}.log"
	// in these case DATA is a wildcard which may cover multiple path segments
	// to check if this is the case, we need to check if the penultimate layout segments is a wildcard
	if pathLength > (len(layoutParts)-1) && len(layoutParts) > 1 && isWildcard(layoutParts[len(layoutParts)-2]) {
		// if the penultimate segment is a wildcard, just use the layout up to that segment
		// - trim the layout to make this the final segment
		// (i.e. trim off the filename portion)
		fileLayout = strings.Join(layoutParts[:len(layoutParts)-1], "/")
	} else {
		//otherwise just reconstruct the layout to match the path segment length
		if len(layoutParts) > pathLength {
			// Reconstruct the layout to match the path segment length
			// (NOTE: add the trailing slash to ensure we match the full segment)
			fileLayout = strings.Join(layoutParts[:pathLength], "/")
		} else {
			fileLayout = strings.Join(layoutParts, "/")
		}
	}

	// Add a trailing slash to the path segment AND the pattern to ensure we match the full segment
	if !strings.HasSuffix(pathSegment, "/") {
		pathSegment = pathSegment + "/"
	}
	if !strings.HasSuffix(fileLayout, "/") {
		fileLayout = fileLayout + "/"
	}
	// this covers the case where the pattern is "/foo/AWS" and the path is "/foo/AWSLogs" which should fail
	// but will pass without the trailing slashes

	// Extract metadata from the path segment
	return getPathLeafMetadata(g, pathSegment, fileLayout)

}

func isWildcard(s string) bool {
	return strings.Contains(s, "{DATA") || strings.Contains(s, "{GREEDYDATA") || strings.Contains(s, "{NOTSPACE")
}

// getPathLeafMetadata extracts metadata from a path
// based on the file layout, which is a grok pattern
// the grok pattern is assumed to start at the beginning of the path segment
func getPathLeafMetadata(g *grok.Grok, filepath string, layout string) (bool, map[string][]byte, error) {
	err := g.Compile(layout, true)
	if err != nil {
		return false, nil, err
	}
	// first check if the path matches the layout
	if !g.MatchString(filepath) {
		return false, nil, nil
	}
	// if it does, extract the metadata
	metadata, err := g.Parse([]byte(filepath))
	if err != nil {
		return false, nil, err
	}
	return true, metadata, nil
}
