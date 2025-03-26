package table

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
	"sync"
	"time"
)

type CollectorImpl[R types.RowStruct] struct {
	observable.ObservableImpl

	source              row_source.RowSource
	req                 *types.CollectRequest
	status              *events.Status
	lastStatusEventTime time.Time
	// wait group to wait the collection to finish
	// for RowEnrichmentCollector this is incremented when a row is received and decremented when it is processed
	// for ArtifactConversionCollector this is incremented when an artifact is downloaded and decremented when it is processed
	collectionWg sync.WaitGroup

	rowCount   int64
	chunkCount int32
}

func (c *CollectorImpl[R]) Close() {

}

// Collect executes the collection process. Tell our source to start collection
func (c *CollectorImpl[R]) Collect(ctx context.Context) (int64, int32, error) {
	// create empty status event#
	c.status = events.NewStatusEvent(c.req.ExecutionId)

	// tell our source to Collect
	// this is a blocking call, but we will receive and process row events during the execution
	err := c.source.Collect(ctx)
	if err != nil {
		return 0, 0, err
	}

	slog.Info("Source collection complete - waiting for enrichment")
	// wait for the collection to finish
	c.collectionWg.Wait()
	defer slog.Info("Enrichment complete")

	// notify observers of final status
	if err := c.NotifyObservers(ctx, c.status); err != nil {
		slog.Error("tableName RowSourceImpl: error notifying observers of status", "error", err)
	}

	// return the number of rows processed
	return c.rowCount, c.chunkCount, nil

}

// GetFromTime returns the 'resolved' from time of the source
func (c *CollectorImpl[R]) GetFromTime() *row_source.ResolvedFromTime {
	return c.source.GetFromTime()
}

func (c *CollectorImpl[R]) initSource(ctx context.Context, req *types.CollectRequest, sourceMetadata *SourceMetadata[R]) error {
	params := &row_source.RowSourceParams{
		SourceConfigData:    req.SourceData,
		ConnectionData:      req.ConnectionData,
		CollectionStatePath: req.CollectionStatePath,
		From:                req.From,
		CollectionTempDir:   req.CollectionTempDir,
	}

	// ask factory to create and initialise the source for us
	// NOTE: we pass the original
	source, err := row_source.Factory.GetRowSource(ctx, params, sourceMetadata.Options...)
	if err != nil {
		return err
	}

	c.source = source
	return nil
}

// onChunk is called by the we have written a chunk of enriched rows to a [JSONL/CSV] file
// notify observers of the chunk
func (c *CollectorImpl[R]) onChunk(ctx context.Context, chunkNumber int32) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber)

	if err = c.NotifyObservers(ctx, e); err != nil {
		return fmt.Errorf("error notifying observers of chunk: %w", err)
	}

	// tell source to save collection state
	if err := c.source.SaveCollectionState(); err != nil {
		return fmt.Errorf("error saving collection state: %w", err)
	}
	return nil
}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (c *CollectorImpl[R]) updateStatus(ctx context.Context, e events.Event) {
	c.status.Update(e)

	// send a status event periodically
	if time.Since(c.lastStatusEventTime) > events.StatusUpdateInterval {
		// notify observers
		if err := c.NotifyObservers(ctx, c.status); err != nil {
			slog.Error("CollectorImpl: error notifying observers of status", "error", err)
		}
		// update lastStatusEventTime
		c.lastStatusEventTime = time.Now()
	}
}
