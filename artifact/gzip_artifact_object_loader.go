package artifact

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"os"
)

// GzipObjectLoader is an Loader that can extracts an object from a gzip file
type GzipObjectLoader[T any] struct {
}

func NewGzipObjectLoader[T any]() *GzipObjectLoader[T] {
	return &GzipObjectLoader[T]{}
}

func (g GzipObjectLoader[T]) Identifier() string {
	return "gzip_object_loader"
}

// Load implements Loader
// Extracts an object from a gzip file
func (g GzipObjectLoader[T]) Load(ctx context.Context, info *types.ArtifactInfo) ([]any, error) {
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

	var item T
	if err := json.NewDecoder(gzReader).Decode(&item); err != nil {
		return nil, fmt.Errorf("error decoding %s: %w", info.Name, err)
	}

	return []any{item}, nil
}
