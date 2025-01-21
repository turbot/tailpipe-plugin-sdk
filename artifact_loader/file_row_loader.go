package artifact_loader

import (
	"bufio"
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"os"
)

const FileRowLoaderIdentifier = "file_row_loader"

// FileRowLoader is an Loader that can loads a file from a path and extracts the contenst a line at a time
type FileRowLoader struct {
}

func NewFileRowLoader() Loader {
	return &FileRowLoader{}
}

func (g FileRowLoader) Identifier() string {
	return FileRowLoaderIdentifier
}

// Load implements Loader
// Extracts an object from a  file
func (g FileRowLoader) Load(ctx context.Context, info *types.DownloadedArtifactInfo, dataChan chan *types.RowData) error {
	inputPath := info.LocalName
	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", inputPath, err)
	}

	scanner := bufio.NewScanner(f)

	go func() {
		defer func() {
			f.Close()
			close(dataChan)
		}()

		for scanner.Scan() {
			// check context cancellation
			if ctx.Err() != nil {
				break
			}
			// get the line of text and send
			dataChan <- &types.RowData{
				Data: scanner.Text(),
			}
		}

	}()
	return nil
}
