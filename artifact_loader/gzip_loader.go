package artifact_loader

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"io"
	"os"
)

func init() {
	// register loader
	Loaders = append(Loaders, NewGzipLoader)
}

// GzipLoader is an Loader that can extracts a gzip file and returns all the content
type GzipLoader struct {
}

func NewGzipLoader() Loader {
	return &GzipLoader{}
}

func (g GzipLoader) Identifier() string {
	return GzipLoaderIdentifier
}

// Load implements Loader
// Extracts an object from a gzip file
func (g GzipLoader) Load(ctx context.Context, info *types.ArtifactInfo, dataChan chan *types.RowData) error {
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

	fileData, err := io.ReadAll(gzReader)
	if err != nil {
		return fmt.Errorf("error reading %s: %w", info.Name, err)
	}
	go func() {
		dataChan <- &types.RowData{
			Data: fileData,
		}
		close(dataChan)
	}()

	return nil
}
