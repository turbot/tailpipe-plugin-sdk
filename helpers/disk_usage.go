package helpers

import (
	"fmt"
	"os"
)

// GetFolderFileSize returns the total size of all files in the given folder in bytes (single level only)
func GetFolderFileSize(folderPath string) (int64, error) {
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return 0, fmt.Errorf("error reading directory %s: %w", folderPath, err)
	}

	var totalSize int64
	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return 0, fmt.Errorf("error getting file info: %w", err)
			}
			totalSize += info.Size()
		}
	}
	return totalSize, nil
}
