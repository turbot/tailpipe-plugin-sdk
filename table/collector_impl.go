package table

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sethvargo/go-retry"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type CollectorImpl[R types.RowStruct] struct {
	observable.PausableObservableImpl

	source              row_source.RowSource
	req                 *types.CollectRequest
	status              *events.Status
	lastStatusEventTime time.Time
	// wait group to wait the collection to finish
	// for RowEnrichmentCollector this is incremented when a row is received and decremented when it is processed
	// for ArtifactConversionCollector this is incremented when an artifact is downloaded and decremented when it is processed
	collectionWg sync.WaitGroup
	// the location to write JSON files
	jsonPath string

	rowCount   int64
	chunkCount int32

	pollMutex sync.Mutex
}

func (c *CollectorImpl[R]) Close() {

}

// PauseCollection pauses the source and pauses our own event handling
func (c *CollectorImpl[R]) PauseCollection() error {
	slog.Info("CollectorImpl: PauseCollection called")
	if err := c.PausableObservableImpl.Pause(); err != nil {
		return fmt.Errorf("error pausing observable: %w", err)
	}
	return c.source.Pause()
}

// ResumeCollection resumes the source and resumes our own event handling
func (c *CollectorImpl[R]) ResumeCollection() error {
	slog.Info("CollectorImpl: ResumeCollection called")
	if err := c.PausableObservableImpl.Resume(); err != nil {
		return fmt.Errorf("error resuming observable: %w", err)
	}
	return c.source.Resume()
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
		return 0, 0, fmt.Errorf("error notifying observers of final status: %w", err)
	}

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
// notify observers of the chunk and save collection state
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

	// now check the size of the json destination folder and pause the source if it is too large
	return c.checkJsonlSize(ctx)
}

// check the size of the json destination folder and pause the source if it is too large
func (c *CollectorImpl[R]) checkJsonlSize(ctx context.Context) error {
	// Try to acquire the lock, return immediately if already locked
	if !c.pollMutex.TryLock() {
		slog.Debug("JSONL sizeMb polling already in progress")
		return nil
	}
	defer c.pollMutex.Unlock()

	sizeMb, err := helpers.GetFolderFileSizeMb(c.jsonPath)
	if err != nil {
		return fmt.Errorf("error getting jsonl folder sizeMb: %w", err)
	}
	if sizeMb > c.req.MaxJsonlSizeMb {
		slog.Info("JSONL folder max size exceeded - pausing source", "sizeMb", sizeMb, "maxSizeMb", c.req.MaxJsonlSizeMb)
		// pause our own event handling and our sources
		if err := c.PauseCollection(); err != nil {
			return fmt.Errorf("error pausing source: %w", err)
		}
		//  periodically check the sizeMb of the folder and resume the source when it has shrunk
		if err := c.pollJsonlSize(ctx); err != nil {
			return err
		}

	}
	return nil
}

func (c *CollectorImpl[R]) pollJsonlSize(ctx context.Context) error {

	slog.Info("******* pollJsonlSize")
	defer slog.Info("******* pollJsonlSize - done *******")
	err := retry.Do(ctx, retry.NewConstant(5*time.Second), func(ctx context.Context) error {
		// check if context is cancelled
		if err := ctx.Err(); err != nil {
			return nil
		}

		// get the size of the json folder
		sizeMb, err := helpers.GetFolderFileSizeMb(c.jsonPath)
		if err != nil {
			return retry.RetryableError(fmt.Errorf("error getting jsonl folder size: %w", err))
		}

		slog.Info("poll JSONL folder size check", "size Mb", sizeMb)
		// if the size is below the threshold, resume the source
		if sizeMb <= c.req.MaxJsonlSizeMb {

			return nil
		}

		return retry.RetryableError(fmt.Errorf("folder size still above threshold"))
	})

	if err != nil {
		return err
	}

	slog.Info("JSONL folder size is below threshold - resuming source")
	return c.ResumeCollection()
}

// if it is too large, we will delete the oldest file
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
