package artifact_loader

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const GzipRowLoaderIdentifier = "gzip_row_loader"

// GzipRowLoader is an Loader that can extracts an object from a gzip file
type GzipRowLoader struct {
}

func NewGzipRowLoader() Loader {
	return &GzipRowLoader{}
}

func (g GzipRowLoader) Identifier() string {
	return GzipRowLoaderIdentifier
}

// Load implements Loader
// Extracts an object from a gzip file
func (g GzipRowLoader) Load(ctx context.Context, info *types.ArtifactInfo, dataChan chan *types.RowData) error {
	inputPath := info.Name
	gzFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", inputPath, err)
	}

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		gzFile.Close()
		return fmt.Errorf("error creating gzip reader for %s: %w", inputPath, err)
	}

	scanner := bufio.NewScanner(gzReader)

	go func() {
		// ensure to close reader and file
		defer func() {
			gzFile.Close()
			gzReader.Close()
			close(dataChan)
		}()

		for scanner.Scan() {
			// check context cancellation
			if ctx.Err() != nil {
				slog.Info("context cancelled")
				break
			}
			if err := scanner.Err(); err != nil {
				slog.Error("Error while scanning", "error", err)
			}

			// get the line of text and send
			dataChan <- &types.RowData{
				Data: scanner.Text(),
			}
		}
	}()
	return nil
}
