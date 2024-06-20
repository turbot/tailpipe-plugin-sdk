package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"log/slog"
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
const JSONLChunkSize = 10000

// TODO rather than Base, maybe this should be EventManager or something?
// Base should be embedded in all tailpipe plugin implementations
type Base struct {
	observerLock sync.RWMutex
	// Observers is a list of all Observers that are currently connected
	// for now these are just the GRPC stream corresponding to the AddObserver call
	Observers []EventStream

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

// AddObserver implements shared.TailpipePluginServer
// It is the GRPC handler for the AddObserver call
func (p *Base) AddObserver(stream proto.TailpipePlugin_AddObserverServer) error {
	// add to list of Observers
	p.observerLock.Lock()
	p.Observers = append(p.Observers, stream)
	p.observerLock.Unlock()

	// hold stream open
	// TODO do we need a remove observer function, in which case this could wait on a waitgroup associated with the observer?
	select {}
	return nil
}

// OnRow is called by the plugin for every row which it produces
// the row is buffered and written to a JSONL file when the buffer is full
func (p *Base) OnRow(row any, req *proto.CollectRequest) (int, error) {
	if p.rowBufferMap == nil {
		// this musty mean the plugin has overridden the Init function and not called the base
		return 0, errors.New("Base.Init must be called from the plugin Init function")
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
		slog.Debug("writing chunk to JSONL file", "chunk", chunkNumber, "rows", numRows)

		// convert row to a JSONL file
		err := p.writeJSONL(rowsToWrite, req, chunkNumber)
		if err != nil {
			return rowCount, fmt.Errorf("failed to write JSONL file: %w", err)
		}

		p.OnChunk(req, chunkNumber)
	}
	return rowCount, nil
}

// OnStarted is called by the plugin when it starts processing a collection request
// any observers are notified
func (p *Base) OnStarted(req *proto.CollectRequest) error {
	// construct proto event
	return p.notifyObservers(proto.NewStartedEvent(req.ExecutionId))
}

// OnChunk is called by the plugin when it has written a chunk of rows to a JSONL file
func (p *Base) OnChunk(req *proto.CollectRequest, chunkNumber int) error {
	// construct proto event
	return p.notifyObservers(proto.NewChunkWrittenEvent(req.ExecutionId, chunkNumber))
}

// OnComplete is called by the plugin when it has finished processing a collection request
// remaining rows are written and any observers are notified
func (p *Base) OnComplete(req *proto.CollectRequest, err error) error {
	// write any  remaining rows (call OnRow with a nil row)
	// NOTE: this returns the row count
	rowCount, err := p.OnRow(nil, req)
	if err != nil {
		return err
	}

	// notify observers of completion
	// figure out the number of chunks written, including partial chunks
	chunksWritten := int(rowCount / JSONLChunkSize)
	if rowCount%JSONLChunkSize > 0 {
		chunksWritten++
	}

	return p.notifyObservers(proto.NewCompleteEvent(req.ExecutionId, rowCount, chunksWritten, err))
}

func (p *Base) notifyObservers(e *proto.Event) error {
	p.observerLock.RLock()
	defer p.observerLock.RUnlock()
	var notifyErrors []error
	for _, observer := range p.Observers {
		observer.Send(e)
	}

	return errors.Join(notifyErrors...)
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
