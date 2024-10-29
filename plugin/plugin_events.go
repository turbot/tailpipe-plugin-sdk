package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
)

// Notify implements observable.Observer
func (b *Plugin) Notify(ctx context.Context, event events.Event) error {
	switch e := event.(type) {
	case *events.Row:
		return b.handleRowEvent(ctx, e)
	case *events.Status:
		return b.NotifyObservers(ctx, e)
	default:
		return fmt.Errorf("unexpected event type: %T", e)
	}
}

// OnStarted is called by the plugin when it starts processing a collection request
// any observers are notified
func (b *Plugin) OnStarted(ctx context.Context, executionId string) error {
	return b.NotifyObservers(ctx, events.NewStartedEvent(executionId))
}

// OnChunk is called by the plugin when it has written a chunk of enriched rows to a [JSONL/CSV] file
func (b *Plugin) OnChunk(ctx context.Context, chunkNumber int, paging json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber, paging)

	return b.NotifyObservers(ctx, e)
}

// handleRowEvent is called by the plugin for every row which it produces
// the row is buffered and written to a JSONL file when the buffer is full
func (b *Plugin) handleRowEvent(ctx context.Context, e *events.Row) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	if b.rowBufferMap == nil {
		// this must mean the plugin has overridden the Init function and not called the base
		// this should be prevented by the validation test
		return errors.New("RowSourceBase.Init must be called from the plugin Init function")
	}
	row := e.Row
	if row == nil {
		return fmt.Errorf("plugin.RowSourceBase.handleRowEvent: row is nil")
	}

	// add row to row buffer
	b.rowBufferLock.Lock()

	rowCount := b.rowCountMap[executionId]
	b.rowBufferMap[executionId] = append(b.rowBufferMap[executionId], row)
	rowCount++
	b.rowCountMap[executionId] = rowCount

	var rowsToWrite []any
	if len(b.rowBufferMap[executionId]) == JSONLChunkSize {
		rowsToWrite = b.rowBufferMap[executionId]
		b.rowBufferMap[executionId] = nil
	}
	b.rowBufferLock.Unlock()

	if numRowsToWrite := len(rowsToWrite); numRowsToWrite > 0 {
		return b.writeChunk(ctx, rowCount, rowsToWrite, e.CollectionState)
	}

	return nil
}

func (b *Plugin) writeChunk(ctx context.Context, rowCount int, rowsToWrite []any, collectionState json.RawMessage) error {
	// determine chunk number from rowCountMap
	chunkNumber := int(rowCount / JSONLChunkSize)
	// check for final partial chunk
	if rowCount%JSONLChunkSize > 0 {
		chunkNumber++
	}
	slog.Debug("writing chunk to JSONL file", "chunk", chunkNumber, "rows", len(rowsToWrite))

	// convert row to a JSONL file
	err := b.writer.WriteChunk(ctx, rowsToWrite, chunkNumber)
	if err != nil {
		slog.Error("failed to write JSONL file", "error", err)
		return fmt.Errorf("failed to write JSONL file: %w", err)
	}
	// notify observers, passing the collection state data
	return b.OnChunk(ctx, chunkNumber, collectionState)
}
