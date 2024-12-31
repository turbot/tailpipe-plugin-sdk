package artifact_source

import (
	"fmt"
	"github.com/elastic/go-grok"
	"github.com/turbot/pipe-fittings/filter"
	"strings"
)

// MetadataSatisfiesFilters checks if a path segment satisfies the given filters,
// based on the file layout, which is a grok pattern
// the grok pattern is assumed to start at the beginning of the path segment
func MetadataSatisfiesFilters(metadata map[string]string, filters map[string]*filter.SqlFilter) bool {
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

// GetPathSegmentMetadata extracts metadata from a path segment
// based on the file layout, which is a grok pattern
// the grok pattern is assumed to start at the beginning of the path segment
// - it is trimmed to the length of the path segment
func GetPathSegmentMetadata(g *grok.Grok, pathSegment, fileLayout string) (bool, map[string][]byte, error) {
	// Split and truncate the file layout to match the path segment's length
	pathParts := strings.Split(pathSegment, "/")
	layoutParts := strings.Split(fileLayout, "/")
	pathLength := len(pathParts)

	if pathLength > len(layoutParts) {
		// The layout doesn't match the path length
		return false, nil, fmt.Errorf("path segment length exceeds layout length")
	}

	// if the path part is empty or is ".", it must be the first segment, do not try to match
	if pathParts[0] == "" || pathParts[0] == "." {
		// marks as matching
		return true, nil, nil
	}
	// Reconstruct the layout to match the path segment length
	fileLayout = strings.Join(layoutParts[:pathLength], "/")

	// Extract metadata from the path segment
	return GetPathMetadata(g, pathSegment, fileLayout)

}

// GetPathMetadata extracts metadata from a path
// based on the file layout, which is a grok pattern
// the grok pattern is assumed to start at the beginning of the path segment
func GetPathMetadata(g *grok.Grok, filepath string, layout string) (bool, map[string][]byte, error) {
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
