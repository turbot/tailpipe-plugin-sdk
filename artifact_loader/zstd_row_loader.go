package artifact_loader

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const ZstdRowLoaderIdentifier = "zstd_row_loader"

// ZstdRowLoader is a Loader that can extract an object from a zstd file line by line
type ZstdRowLoader struct {
}

func NewZstdRowLoader() Loader {
	return &ZstdRowLoader{}
}

func (z ZstdRowLoader) Identifier() string {
	return ZstdRowLoaderIdentifier
}

// Load implements Loader
// Extracts an object from a zstd file line by line
func (z ZstdRowLoader) Load(ctx context.Context, info *types.DownloadedArtifactInfo, dataChan chan *types.RowData) error {
	slog.Debug("ZstdRowLoader Load", "path", info.LocalName)

	inputPath := info.LocalName
	zstFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", inputPath, err)
	}

	// Create a zstd reader
	zstReader, err := zstd.NewReader(zstFile)
	if err != nil {
		zstFile.Close()
		return fmt.Errorf("error creating zstd reader for %s: %w", inputPath, err)
	}

	scanner := bufio.NewScanner(zstReader)

	go func() {
		// ensure to close reader and file
		defer func() {
			zstFile.Close()
			zstReader.Close()
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
		slog.Debug("ZstdRowLoader Load complete", "path", info.LocalName)
	}()
	return nil
}