package artifact

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"os"
)

// GzipRowLoader is an Loader that can extracts an object from a gzip file
type GzipRowLoader struct {
}

func NewGzipRowLoader() (Loader, error) {
	return &GzipRowLoader{}, nil
}

func (g GzipRowLoader) Identifier() string {
	return GzipRowLoaderIdentifier
}

// Load implements Loader
// Extracts an object from a gzip file
func (g GzipRowLoader) Load(ctx context.Context, info *types.ArtifactInfo) ([]any, error) {
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

	scanner := bufio.NewScanner(gzReader)

	var res []any
	for scanner.Scan() {
		// check context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// todo stream data / convert to iterator
		// get the line of text and append to the result
		res = append(res, scanner.Text())
	}
	return res, nil
}
