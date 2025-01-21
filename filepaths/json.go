package filepaths

import (
	"fmt"

	"os"
	"path/filepath"
)

// EnsureJSONLPath ensures the source path exists - this is the folder where the plugin writes JSONL files
func EnsureJSONLPath(baseDir string) (string, error) {
	sourceFilePath := filepath.Join(baseDir, "source")
	// ensure it exists
	if _, err := os.Stat(sourceFilePath); os.IsNotExist(err) {
		err = os.MkdirAll(sourceFilePath, 0755)
		if err != nil {
			return "", fmt.Errorf("could not create source directory %s: %w", sourceFilePath, err)
		}
	}

	return sourceFilePath, nil
}
