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

// EnsureArtifactPath ensures the artifact temp dir path exists - this is the folder where the artifact source writes downloaded files
func EnsureArtifactPath(baseDir string) (string, error) {
	artifactPath := filepath.Join(baseDir, "artifacts")

	// ensure it exists
	if _, err := os.Stat(artifactPath); os.IsNotExist(err) {
		err = os.MkdirAll(artifactPath, 0755)
		if err != nil {
			return "", fmt.Errorf("could not create artifact directory %s: %w", artifactPath, err)
		}
	}

	return artifactPath, nil
}
