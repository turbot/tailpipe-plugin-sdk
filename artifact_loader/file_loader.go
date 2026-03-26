package artifact_loader

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"io"
	"os"
)

const FileLoaderIdentifier = "file_loader"

// FileLoader is an Loader that can loads a file from a path and extracts all the content
type FileLoader struct {
}

func NewFileLoader() Loader {
	return &FileLoader{}
}

func (g FileLoader) Identifier() string {
	return FileLoaderIdentifier
}

// Load implements [Loader]
// Extracts an object from a  file
func (g FileLoader) Load(_ context.Context, info *types.DownloadedArtifactInfo, dataChan chan *types.RowData) error {
	inputPath := info.LocalName
	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", inputPath, err)
	}
	defer f.Close()

	fileData, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("error reading %s: %w", info.LocalName, err)
	}

	go func() {
		dataChan <- &types.RowData{
			Data: fileData,
		}
		close(dataChan)
	}()

	return nil
}
