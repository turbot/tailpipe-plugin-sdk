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
func (g GzipRowLoader) Load(ctx context.Context, info *types.ArtifactInfo, dataChan chan *ArtifactData) error {
	inputPath := info.Name
	gzFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", inputPath, err)
	}
	defer gzFile.Close()

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return fmt.Errorf("error creating gzip reader for %s: %w", inputPath, err)
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)

	go func() {
		for scanner.Scan() {
			// check context cancellation
			if ctx.Err() != nil {
				break
			}
			// get the line of text and send
			dataChan <- &ArtifactData{
				Data: scanner.Text(),
			}
		}
		close(dataChan)
	}()
	return nil
}
