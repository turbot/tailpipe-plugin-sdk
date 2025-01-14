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

func CollectionStatePath(collectionFolder string, table, partition string) string {
	// NOTE: the collectionFolder is keyed by the PID of the CLI process that initiated the collection
	// it will be of the form ~/.tailpipe/collection/<workspace>/<pid>
	// the collection state is written to the workspace folder, i.e. the parent folder of the PID folder
	baseDir := filepath.Dir(collectionFolder)
	// return the path to the collection state file
	return filepath.Join(baseDir, fmt.Sprintf("collection_state_%s_%s.json", table, partition))
}
