package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
)

// Notify implements observable.Observer
func (p *PluginImpl) Notify(ctx context.Context, event events.Event) error {
	switch e := event.(type) {

	case *events.Status, *events.Error:
		return p.NotifyObservers(ctx, e)
	case *events.Chunk:
		// NOTE: this will only be received if the table is directly converting an artifact to JSONL
		// e.g. a csv file based table
		return p.OnChunk(ctx, e.ChunkNumber, e.CollectionState)
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
