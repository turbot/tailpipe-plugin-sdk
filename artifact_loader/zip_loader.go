package artifact_loader

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const ZipLoaderIdentifier = "zip_loader"

// ZipLoader is a Loader that extracts a zip file and returns all the content
type ZipLoader struct {
}

func NewZipLoader() Loader {
	return &ZipLoader{}
}

func (z ZipLoader) Identifier() string {
	return ZipLoaderIdentifier
}

// Load implements Loader
// Extracts an object from a zip file
func (z ZipLoader) Load(ctx context.Context, info *types.DownloadedArtifactInfo, dataChan chan *types.RowData) error {
	slog.Debug("ZipLoader Load", "path", info.LocalName)
	inputPath := info.LocalName

	// Open the zip file
	zipFile, err := zip.OpenReader(inputPath)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", inputPath, err)
	}
	defer zipFile.Close()

	// Check if the zip file has any files
	if len(zipFile.File) == 0 {
		return fmt.Errorf("zip file %s is empty", inputPath)
	}

	// Check if the zip file has more than one file
	// TODO: Handle multiple files in the zip archive: https://github.com/turbot/tailpipe-plugin-sdk/issues/198
	if len(zipFile.File) > 1 {
		return fmt.Errorf("zip file %s contains more than one file and can't be processed", inputPath)
	}

	f := zipFile.File[0]

	// Open the file inside the zip
	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("error opening file inside zip %s: %w", f.Name, err)
	}
	defer rc.Close()

	// Read all the content
	fileData, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("error reading file inside zip %s: %w", f.Name, err)
	}

	go func() {
		dataChan <- &types.RowData{
			Data: fileData,
		}
		close(dataChan)

		slog.Debug("ZipLoader Load complete", "path", info.LocalName)
	}()

	return nil
}
