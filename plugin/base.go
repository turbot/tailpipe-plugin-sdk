package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// how may rows to write in each JSONL file
// TODO configure?
const JSONLChunkSize = 10000

// Base should be embedded in all tailpipe plugin implementations
type Base struct {
	observable.Base

	// row buffer keyed by execution id
	// each row buffer is used to write a JSONL file
	rowBufferMap map[string][]any
	// mutex for row buffer map AND rowCountMap
	rowBufferLock sync.RWMutex

	// map of row counts keyed by execution id
	rowCountMap map[string]int

	// map of collection constructors
	collectionFactory map[string]func() Collection

	// map of collection schemas
	schemaMap schema.SchemaMap
}

// Init implements TailpipePlugin. It is called by Serve when the plugin is started
// if the plugin overrides this function it must call the base implementation
func (b *Base) Init(context.Context) error {
	b.rowBufferMap = make(map[string][]any)
	b.rowCountMap = make(map[string]int)
	return nil
}

// Shutdown implements TailpipePlugin. It is called by Serve when the plugin exits
func (b *Base) Shutdown(context.Context) error {
	return nil
}

// GetSchema implements TailpipePlugin
func (b *Base) GetSchema() schema.SchemaMap {
	return b.schemaMap
}

func (b *Base) getRowCount(req *proto.CollectRequest) (int, int) {
	// get rowcount
	b.rowBufferLock.RLock()
	rowCount := b.rowCountMap[req.ExecutionId]
	b.rowBufferLock.RUnlock()

	// notify observers of completion
	// figure out the number of chunks written, including partial chunks
	chunksWritten := int(rowCount / JSONLChunkSize)
	if rowCount%JSONLChunkSize > 0 {
		chunksWritten++
	}
	return rowCount, chunksWritten
}

func (b *Base) writeJSONL(ctx context.Context, rows []any, chunkNumber int) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	destPath, err := context_values.DestPathFromContext(ctx)
	if err != nil {
		return err
	}

	// generate the filename
	filename := filepath.Join(destPath, ExecutionIdToFileName(executionId, chunkNumber))

	// Open the file for writing
	file, err := os.Create(filename)
	if err != nil {
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
			return fmt.Errorf("failed to encode item: %w", err)
		}
	}

	return nil
}

func (b *Base) RegisterCollections(collectionFunc ...func() Collection) error {
	// create the maps
	b.collectionFactory = make(map[string]func() Collection)
	b.schemaMap = make(map[string]*schema.RowSchema)

	commonSchema, err := schema.SchemaFromStruct(enrichment.CommonFields{})
	if err != nil {
		return fmt.Errorf("failed to create schema for common fields: %w", err)
	}
	errs := make([]error, 0)
	for _, ctor := range collectionFunc {
		c := ctor()
		// register the collection
		b.collectionFactory[c.Identifier()] = ctor

		// get the schema for the collection row type
		rowStruct := c.GetRowStruct()
		s, err := schema.SchemaFromStruct(rowStruct)
		if err != nil {
			errs = append(errs, err)
		}
		// merge in the common schema
		s.Merge(commonSchema)
		b.schemaMap[c.Identifier()] = s
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil

}
