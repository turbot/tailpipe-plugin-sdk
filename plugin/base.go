package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"os"
	"path/filepath"
	"sync"
)

// TODO we need validate the rows types provided by the plugin to ensure they are valid
// maybe the plugin should register collections and there should be validation code to validate each collection entity
/*
GetConnection() string
	GetYear() int
	GetMonth() int
	GetDay() int
	GetTpID() string
	GetTpTimestamp() int64
*/

// how may rows to write in each JSONL file
// TODO configure?
const JSONLChunkSize = 1000

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
}

// Init implements TailpipePlugin. It is called by Serve when the plugin is started
// if the plugin overrides this function it must call the base implementation
func (p *Base) Init(context.Context) error {
	p.rowBufferMap = make(map[string][]any)
	p.rowCountMap = make(map[string]int)
	return nil
}

// Shutdown implements TailpipePlugin. It is called by Serve when the plugin exits
func (p *Base) Shutdown(context.Context) error {
	return nil
}

// Notify implements observable.Observer
func (p *Base) Notify(event events.Event) error {
	switch e := event.(type) {
	case *events.Row:
		return p.OnRow(e.Row, e.Request)
	case *events.Started:
		return p.OnStarted(e.Request)
	case *events.Chunk:
		return p.OnChunk(e.Request, e.ChunkNumber)
	case *events.Completed:
		return p.OnComplete(e.Request, e.Err)
	default:
		return fmt.Errorf("unknown event type: %T", e)
	}
}

// OnRow is called by the plugin for every row which it produces
// the row is buffered and written to a JSONL file when the buffer is full
func (p *Base) OnRow(row any, req *proto.CollectRequest) error {
	if p.rowBufferMap == nil {
		// this musty mean the plugin has overridden the Init function and not called the base
		return errors.New("Base.Init must be called from the plugin Init function")
	}

	// add row to row buffer
	p.rowBufferLock.Lock()

	rowCount := p.rowCountMap[req.ExecutionId]
	if row != nil {
		p.rowBufferMap[req.ExecutionId] = append(p.rowBufferMap[req.ExecutionId], row)
		rowCount++
		p.rowCountMap[req.ExecutionId] = rowCount
	}

	var rowsToWrite []any
	if row == nil || len(p.rowBufferMap[req.ExecutionId]) == JSONLChunkSize {
		rowsToWrite = p.rowBufferMap[req.ExecutionId]
		p.rowBufferMap[req.ExecutionId] = nil
	}
	p.rowBufferLock.Unlock()

	if numRows := len(rowsToWrite); numRows > 0 {
		// determine chunk number from rowCountMap
		chunkNumber := int(rowCount / JSONLChunkSize)
		// check for final partial chunk
		if rowCount%JSONLChunkSize > 0 {
			chunkNumber++
		}
		//slog.Debug("writing chunk to JSONL file", "chunk", chunkNumber, "rows", numRows)

		// convert row to a JSONL file
		err := p.writeJSONL(rowsToWrite, req, chunkNumber)
		if err != nil {
			return fmt.Errorf("failed to write JSONL file: %w", err)
		}

		p.OnChunk(req, chunkNumber)
	}
	return nil
}

// OnStarted is called by the plugin when it starts processing a collection request
// any observers are notified
func (p *Base) OnStarted(req *proto.CollectRequest) error {
	// construct proto event
	return p.NotifyObservers(events.NewStartedEvent(req))
}

// OnChunk is called by the plugin when it has written a chunk of enriched rows to a [JSONL/CSV] file
func (p *Base) OnChunk(req *proto.CollectRequest, chunkNumber int) error {
	// construct proto event
	return p.NotifyObservers(events.NewChunkEvent(req, chunkNumber))
}

// OnComplete is called by the plugin when it has finished processing a collection request
// remaining rows are written and any observers are notified
func (p *Base) OnComplete(req *proto.CollectRequest, err error) error {
	if err == nil {
		// write any  remaining rows (call OnRow with a nil row)
		// NOTE: this returns the row count
		err = p.OnRow(nil, req)
	}

	rowCount, chunksWritten := p.getRowCount(req)

	return p.NotifyObservers(events.NewCompletedEvent(req, rowCount, chunksWritten, err))
}

func (p *Base) getRowCount(req *proto.CollectRequest) (int, int) {
	// get rowcount
	p.rowBufferLock.RLock()
	rowCount := p.rowCountMap[req.ExecutionId]
	p.rowBufferLock.RUnlock()

	// notify observers of completion
	// figure out the number of chunks written, including partial chunks
	chunksWritten := int(rowCount / JSONLChunkSize)
	if rowCount%JSONLChunkSize > 0 {
		chunksWritten++
	}
	return rowCount, chunksWritten
}

func (p *Base) writeJSONL(rows []any, req *proto.CollectRequest, chunkNumber int) error {
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
