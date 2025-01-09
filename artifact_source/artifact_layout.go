package artifact_source

import (
	"fmt"
	"github.com/elastic/go-grok"
	"github.com/turbot/pipe-fittings/filter"
	"path/filepath"
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

// GetPathMetadata get the metadata from the given file path, based on the file layout
// returns whether the path matches the layout pattern, and the medata map
func GetPathMetadata(targetPath, basePath string, layout *string, isDir bool, g *grok.Grok) (bool, map[string]string, error) {
	if layout == nil {
		return false, nil, nil
	}
	// remove the base path from the path
	relPath, err := filepath.Rel(basePath, targetPath)
	if err != nil {
		return false, nil, err
	}

	// if this is a directory, we just want to evaluate the pattern segments up to this directory
	// so call getPathSegmentMetadata which trims the pattern to match the path length
	var f func(g *grok.Grok, pathSegment, fileLayout string) (bool, map[string][]byte, error)
	if isDir {
		f = getPathSegmentMetadata
	} else {
		f = getPathLeafMetadata
	}
	match, metadata, err := f(g, relPath, *layout)
	if err != nil {
		return false, nil, err
	}

	// convert the metadata to a string map
	return match, ByteMapToStringMap(metadata), nil
}

func ByteMapToStringMap(m map[string][]byte) map[string]string {
	res := make(map[string]string, len(m))
	for k, v := range m {
		res[k] = string(v)
	}
	return res
}

// getPathSegmentMetadata extracts metadata from a path segment
// based on the file layout, which is a grok pattern
// the grok pattern is assumed to start at the beginning of the path segment
// - it is trimmed to the length of the path segment
func getPathSegmentMetadata(g *grok.Grok, pathSegment, fileLayout string) (bool, map[string][]byte, error) {
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
	return getPathLeafMetadata(g, pathSegment, fileLayout)

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
