package artifact_loader

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const ZstdLoaderIdentifier = "zstd_loader"

// ZstdLoader is a Loader that can extract a zstd file and returns all the content
type ZstdLoader struct {
}

func NewZstdLoader() Loader {
	return &ZstdLoader{}
}

func (z ZstdLoader) Identifier() string {
	return ZstdLoaderIdentifier
}

// Load implements Loader
// Extracts an object from a zstd file
func (z ZstdLoader) Load(_ context.Context, info *types.DownloadedArtifactInfo, dataChan chan *types.RowData) error {
	slog.Debug("ZstdLoader Load", "path", info.LocalName)
	inputPath := info.LocalName
	zstFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", inputPath, err)
	}
	defer zstFile.Close()

	// Create a zstd reader
	zstReader, err := zstd.NewReader(zstFile)
	if err != nil {
		return fmt.Errorf("error creating zstd reader for %s: %w", inputPath, err)
	}
	defer zstReader.Close()

	// Read all the content
	fileData, err := io.ReadAll(zstReader)
	if err != nil {
		return fmt.Errorf("error reading %s: %w", info.LocalName, err)
	}
	
	go func() {
		dataChan <- &types.RowData{
			Data: fileData,
		}
		close(dataChan)

		slog.Debug("ZstdLoader Load complete", "path", info.LocalName)
	}()

	return nil
}