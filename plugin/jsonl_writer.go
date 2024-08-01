package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"log/slog"
	"os"
	"path/filepath"
)

// JSONLWriter implements [ChunkWriter] and writes rows to JSONL files
type JSONLWriter struct {
	// the path to write the JSONL files to
	destPath string
}

func NewJSONLWriter(destPath string) ChunkWriter {
	return JSONLWriter{destPath: destPath}
}

func (j JSONLWriter) WriteChunk(ctx context.Context, rows []any, chunkNumber int) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	// generate the filename
	filename := filepath.Join(j.destPath, ExecutionIdToFileName(executionId, chunkNumber))

	// Open the file for writing
	file, err := os.Create(filename)
	if err != nil {
		slog.Error("failed to create JSONL file", "error", err)
		return fmt.Errorf("failed to create JSONL file %s: %w", filename, err)
	}
	defer file.Close()

	slog.Debug("writing JSONL file", "file", filename, "rows", len(rows))
	// Create a JSON encoder
	encoder := json.NewEncoder(file)

	// Iterate over the data slice and write each item as a JSON object
	for _, item := range rows {
		err := encoder.Encode(item)
		if err != nil {
			slog.Error("failed to encode item", "error", err)
			return fmt.Errorf("failed to encode item: %w", err)
		}
	}

	return nil
}
