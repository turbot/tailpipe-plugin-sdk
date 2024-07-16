package plugin

import (
	"context"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"log/slog"
)

// Notify implements observable.Observer
func (b *Base) Notify(ctx context.Context, event events.Event) error {
	switch e := event.(type) {
	case *events.Row:
		return b.handleRowEvent(ctx, e.Row, e.Request)
	default:
		return fmt.Errorf("unexpected event type: %T", e)
	}
}

// OnStarted is called by the plugin when it starts processing a collection request
// any observers are notified
func (b *Base) OnStarted(ctx context.Context, req *proto.CollectRequest) error {
	return b.NotifyObservers(ctx, events.NewStartedEvent(req))
}

// OnChunk is called by the plugin when it has written a chunk of enriched rows to a [JSONL/CSV] file
func (b *Base) OnChunk(ctx context.Context, req *proto.CollectRequest, chunkNumber int) error {
	// construct proto event
	return b.NotifyObservers(ctx, events.NewChunkEvent(req, chunkNumber))
}

// OnCompleted is called by the plugin when it has finished processing a collection request
// remaining rows are written and any observers are notified
func (b *Base) OnCompleted(ctx context.Context, req *proto.CollectRequest, err error) error {
	if err == nil {
		// write any  remaining rows (call handleRowEvent with a nil row)
		// NOTE: this returns the row count
		err = b.handleRowEvent(ctx, nil, req)
	}

	rowCount, chunksWritten := b.getRowCount(req)

	return b.NotifyObservers(ctx, events.NewCompletedEvent(req, rowCount, chunksWritten, err))
}

// handleRowEvent is called by the plugin for every row which it produces
// the row is buffered and written to a JSONL file when the buffer is full
func (b *Base) handleRowEvent(ctx context.Context, row any, req *proto.CollectRequest) error {
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

		return b.OnChunk(ctx, req, chunkNumber)
	}
	return nil
}
