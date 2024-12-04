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
func (p *PluginImpl) Notify(ctx context.Context, event events.Event) error {
	switch e := event.(type) {
	case *events.Row:
		return p.handleRowEvent(ctx, e)
	case *events.Status:
		return p.NotifyObservers(ctx, e)
	default:
		return fmt.Errorf("unexpected event type: %T", e)
	}
}

// OnStarted is called by the plugin when it starts processing a collection request
// any observers are notified
func (p *PluginImpl) OnStarted(ctx context.Context, executionId string) error {
	return p.NotifyObservers(ctx, events.NewStartedEvent(executionId))
}

// OnChunk is called by the plugin when it has written a chunk of enriched rows to a [JSONL/CSV] file
func (p *PluginImpl) OnChunk(ctx context.Context, chunkNumber int, paging json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber, paging)

	return p.NotifyObservers(ctx, e)
}

// handleRowEvent is called by the plugin for every row which it produces
// the row is buffered and written to a JSONL file when the buffer is full
func (p *PluginImpl) handleRowEvent(ctx context.Context, e *events.Row) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	if p.rowBufferMap == nil {
		// this must mean the plugin has overridden the Init function and not called the base
		// this should be prevented by the validation test
		return errors.New("RowSourceImpl.Init must be called from the plugin Init function")
	}
	row := e.Row
	if row == nil {
		return fmt.Errorf("plugin.RowSourceImpl.handleRowEvent: row is nil")
	}

	// add row to row buffer
	p.rowBufferLock.Lock()

	rowCount := p.rowCountMap[executionId]
	p.rowBufferMap[executionId] = append(p.rowBufferMap[executionId], row)
	rowCount++
	p.rowCountMap[executionId] = rowCount

	var rowsToWrite []any
	if len(p.rowBufferMap[executionId]) == JSONLChunkSize {
		rowsToWrite = p.rowBufferMap[executionId]
		p.rowBufferMap[executionId] = nil
	}
	p.rowBufferLock.Unlock()

	if numRowsToWrite := len(rowsToWrite); numRowsToWrite > 0 {
		return p.writeChunk(ctx, rowCount, rowsToWrite, e.CollectionState)
	}

	return nil
}

func (p *PluginImpl) writeChunk(ctx context.Context, rowCount int, rowsToWrite []any, collectionState json.RawMessage) error {
	// determine chunk number from rowCountMap
	chunkNumber := rowCount / JSONLChunkSize
	// check for final partial chunk
	if rowCount%JSONLChunkSize > 0 {
		chunkNumber++
	}
	slog.Debug("writing chunk to JSONL file", "chunk", chunkNumber, "rows", len(rowsToWrite))

	// convert row to a JSONL file
	err := p.writer.WriteChunk(ctx, rowsToWrite, chunkNumber)
	if err != nil {
		slog.Error("failed to write JSONL file", "error", err)
		return fmt.Errorf("failed to write JSONL file: %w", err)
	}
	// notify observers, passing the collection state data
	return p.OnChunk(ctx, chunkNumber, collectionState)
}
