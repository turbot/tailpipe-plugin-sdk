package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/events"
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

// Notify implements observable.Observer
func (b *Base) Notify(event events.Event) error {
	switch e := event.(type) {
	case *events.Row:
		return b.OnRow(e.Row, e.Request)
	case *events.Started:
		return b.OnStarted(e.Request)
	case *events.Chunk:
		return b.OnChunk(e.Request, e.ChunkNumber)
	case *events.Completed:
		return b.OnComplete(e.Request, e.Err)
	default:
		return fmt.Errorf("unknown event type: %T", e)
	}
}

// GetSchema implements TailpipePlugin
func (b *Base) GetSchema() schema.SchemaMap {
	return b.schemaMap
}

// OnRow is called by the plugin for every row which it produces
// the row is buffered and written to a JSONL file when the buffer is full
func (b *Base) OnRow(row any, req *proto.CollectRequest) error {
	if b.rowBufferMap == nil {
		// this musty mean the plugin has overridden the Init function and not called the base
		return errors.New("Base.Init must be called from the plugin Init function")
	}

	// add row to row buffer
	b.rowBufferLock.Lock()

	rowCount := b.rowCountMap[req.ExecutionId]
	if row != nil {
		b.rowBufferMap[req.ExecutionId] = append(b.rowBufferMap[req.ExecutionId], row)
		rowCount++
		b.rowCountMap[req.ExecutionId] = rowCount
	}

	var rowsToWrite []any
	if row == nil || len(b.rowBufferMap[req.ExecutionId]) == JSONLChunkSize {
		rowsToWrite = b.rowBufferMap[req.ExecutionId]
		b.rowBufferMap[req.ExecutionId] = nil
	}
	b.rowBufferLock.Unlock()

	if numRows := len(rowsToWrite); numRows > 0 {
		// determine chunk number from rowCountMap
		chunkNumber := int(rowCount / JSONLChunkSize)
		// check for final partial chunk
		if rowCount%JSONLChunkSize > 0 {
			chunkNumber++
		}
		slog.Debug("writing chunk to JSONL file", "chunk", chunkNumber, "rows", numRows)

		// convert row to a JSONL file
		err := b.writeJSONL(rowsToWrite, req, chunkNumber)
		if err != nil {
			slog.Error("failed to write JSONL file", "error", err)
			return fmt.Errorf("failed to write JSONL file: %w", err)
		}

		b.OnChunk(req, chunkNumber)
	}
	return nil
}

// OnStarted is called by the plugin when it starts processing a collection request
// any observers are notified
func (b *Base) OnStarted(req *proto.CollectRequest) error {
	// construct proto event
	return b.NotifyObservers(events.NewStartedEvent(req))
}

// OnChunk is called by the plugin when it has written a chunk of enriched rows to a [JSONL/CSV] file
func (b *Base) OnChunk(req *proto.CollectRequest, chunkNumber int) error {
	// construct proto event
	return b.NotifyObservers(events.NewChunkEvent(req, chunkNumber))
}

// OnComplete is called by the plugin when it has finished processing a collection request
// remaining rows are written and any observers are notified
func (b *Base) OnComplete(req *proto.CollectRequest, err error) error {
	if err == nil {
		// write any  remaining rows (call OnRow with a nil row)
		// NOTE: this returns the row count
		err = b.OnRow(nil, req)
	}

	rowCount, chunksWritten := b.getRowCount(req)

	return b.NotifyObservers(events.NewCompletedEvent(req, rowCount, chunksWritten, err))
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

func (b *Base) writeJSONL(rows []any, req *proto.CollectRequest, chunkNumber int) error {
	executionId := req.ExecutionId
	destPath := req.OutputPath

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
