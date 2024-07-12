package artifact

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"os"
)

// define enume for extraction strategy
type ExtractionStrategy int

const (
	// Extract the entire object from the artifact
	ExtractObject ExtractionStrategy = iota
	// Extract rows from the artifact
	ExtractRows
)

// GzipExtractorSource is an Loader that can extracts an object from a gzip file
type GzipExtractorSource struct {
	strategy ExtractionStrategy
}

func NewGzipExtractorSource(strategy ExtractionStrategy) *GzipExtractorSource {
	return &GzipExtractorSource{
		strategy: strategy,
	}
}

// Load implements Loader
// Extracts an object from a gzip file
func (g GzipExtractorSource) Load(ctx context.Context, info *types.ArtifactInfo) ([]any, error) {
	inputPath := info.Name
	gzFile, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("error opening %s: %w", inputPath, err)
	}
	defer gzFile.Close()

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return nil, fmt.Errorf("error creating gzip reader for %s: %w", inputPath, err)
	}
	defer gzReader.Close()

	// now depending on the extraction strategy, we may need to decode the object or send line by line
	switch g.strategy {
	case ExtractObject:
		return g.extractObject(info, gzReader)
	case ExtractRows:
		return g.extractRows(ctx, info, gzReader)
	default:
		return nil, fmt.Errorf("unknown extraction strategy: %d", g.strategy)
	}
}

func (g GzipExtractorSource) extractObject(info *types.ArtifactInfo, gzReader *gzip.Reader) ([]any, error) {
	var item any
	if err := json.NewDecoder(gzReader).Decode(&item); err != nil {
		return nil, fmt.Errorf("error decoding %s: %w", info.Name, err)
	}

	return []any{item}, nil
}

func (g GzipExtractorSource) extractRows(ctx context.Context, info *types.ArtifactInfo, gzReader *gzip.Reader) ([]any, error) {
	scanner := bufio.NewScanner(gzReader)

	var res []any
	for scanner.Scan() {
		// check context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// get the line of text and append to the result
		res = append(res, scanner.Text())
	}
	return res, nil
}
