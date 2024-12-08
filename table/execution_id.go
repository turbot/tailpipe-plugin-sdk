package table

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// ExecutionIdToFileName convert an execution id and chunk number to a filename
// assuming a convention of <executionId>-<chunkNumber>.jsonl
func ExecutionIdToFileName(executionId string, chunkNumber int) string {
	return fmt.Sprintf("%s-%d.jsonl", executionId, chunkNumber)
}

// FileNameToExecutionId convert a filename to an execution id
// assuming a convention of <executionId>-<chunkNumber>.jsonl
func FileNameToExecutionId(filename string) (string, int, error) {
	// remove path
	filename = filepath.Base(filename)
	// remove extension
	filename = filename[:len(filename)-len(".jsonl")]
	// get  chunk number

	// find the last dash
	lastDash := strings.LastIndex(filename, "-")
	if lastDash == -1 {
		return "", 0, fmt.Errorf("invalid filename %s", filename)
	}
	executionId := filename[:lastDash]
	chunkNumber, err := strconv.Atoi(filename[lastDash+1:])
	if err != nil {
		return "", 0, fmt.Errorf("invalid filename %s", filename)
	}
	return executionId, chunkNumber, nil
}
