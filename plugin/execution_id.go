package plugin

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ExecutionIdToFileName convert an execution id and chunk number to a filename
// assuming a convention of <executionId>-<chunkNumber>.jsonl
func ExecutionIdToFileName(executionId string, chunkNumber int) string {
	return fmt.Sprintf("%s-%d.jsonl", executionId, chunkNumber)
}

// FileNameToExecutionId convert a filename to an execution id
// assuming a convention of <executionId>-<chunkNumber>.jsonl
func FileNameToExecutionId(filename string) (string, error) {
	// remove path
	filename = filepath.Base(filename)
	// remove extension
	filename = filename[:len(filename)-len(".jsonl")]
	// remove chunk number
	// find the last dash
	lastDash := strings.LastIndex(filename, "-")
	if lastDash == -1 {
		return "", fmt.Errorf("invalid filename %s", filename)
	}
	executionId := filename[:lastDash]
	return executionId, nil
}
