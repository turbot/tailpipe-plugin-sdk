package artifact_loader

import (
	"archive/zip"
	"bufio"
	"context"
	"fmt"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const ZipRowLoaderIdentifier = "zip_row_loader"

// ZipRowLoader is a Loader that extracts a zip file and reads it line by line
type ZipRowLoader struct {
}

func NewZipRowLoader() Loader {
	return &ZipRowLoader{}
}

func (z ZipRowLoader) Identifier() string {
	return ZipRowLoaderIdentifier
}

// Load implements Loader
// Extracts an object from a zip file and reads it line by line
func (z ZipRowLoader) Load(ctx context.Context, info *types.DownloadedArtifactInfo, dataChan chan *types.RowData) error {
	slog.Debug("ZipRowLoader Load", "path", info.LocalName)
	inputPath := info.LocalName

	// Open the zip file
	zipFile, err := zip.OpenReader(inputPath)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", inputPath, err)
	}

	// Check if the zip file has any files
	if len(zipFile.File) == 0 {
		zipFile.Close()
		return fmt.Errorf("zip file %s is empty", inputPath)
	}

	// Check if the zip file has more than one file
	// TODO: Handle multiple files in the zip archive: https://github.com/turbot/tailpipe-plugin-sdk/issues/198
	if len(zipFile.File) > 1 {
		zipFile.Close()
		return fmt.Errorf("zip file %s contains more than one file and can't be processed", inputPath)
	}

	f := zipFile.File[0]

	// Open the file inside the zip
	rc, err := f.Open()
	if err != nil {
		zipFile.Close()
		return fmt.Errorf("error opening file inside zip %s: %w", f.Name, err)
	}

	scanner := bufio.NewScanner(rc)

	go func() {
		// ensure to close reader and file
		defer func() {
			rc.Close()
			zipFile.Close()
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
		slog.Debug("ZipRowLoader Load complete", "path", info.LocalName)
	}()
	return nil
}
