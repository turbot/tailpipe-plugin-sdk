package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// how ofted to send status events
const statusUpdateInterval = 250 * time.Millisecond

// CollectionBase provides a base implementation of the [collection.Collection] interface
// it should be embedded in all Collection implementations
type CollectionBase[T parse.Config] struct {
	observable.ObservableBase

	// the row Source
	Source row_source.RowSource

	// store a reference to the derived collection type so we can call its methods
	impl Collection

	// the collection config
	Config T
	// wait group to wait for all rows to be processed
	// this is incremented each time we receive a row event and decremented when we have processed it
	rowWg sync.WaitGroup

	// we only send status events periodically, to avoid flooding the event stream
	// store a status event and we will update it each time we receive artifact or row events
	status              *events.Status
	lastStatusEventTime time.Time
	statusLock          sync.RWMutex

	enrichTiming types.Timing
}

// Init implements collection.Collection
func (b *CollectionBase[T]) Init(ctx context.Context, collectionConfigData *parse.Data, collectionStateJSON json.RawMessage, sourceConfigData *parse.Data) error {
	if len(collectionConfigData.Hcl) > 0 {
		// parse the config
		var emptyConfig = b.impl.GetConfigSchema().(T)
		c, err := parse.ParseConfig[T](collectionConfigData, emptyConfig)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}
		b.Config = c

		slog.Info("Collection RowSourceBase: config parsed", "config", c)

		// validate config
		if err := c.Validate(); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}
	}
	// initialise the source
	sourceOpts := b.impl.GetSourceOptions()
	// if collectionStateJSON is non-empty, add an option to set it
	if len(collectionStateJSON) > 0 {
		sourceOpts = append(sourceOpts, row_source.WithCollectionStateJSON(collectionStateJSON))
	}
	return b.initSource(ctx, sourceConfigData, sourceOpts...)
}

// initialise the row source
func (b *CollectionBase[T]) initSource(ctx context.Context, configData *parse.Data, sourceOpts ...row_source.RowSourceOption) error {
	// TODO verify we support this source type https://github.com/turbot/tailpipe-plugin-sdk/issues/16

	// now ask plugin to create and initialise the source for us
	source, err := row_source.Factory.GetRowSource(ctx, configData, sourceOpts...)
	if err != nil {
		return err
	}
	b.Source = source

	// add ourselves as an observer to our Source
	return b.Source.AddObserver(b)
}

// RegisterImpl is called by the plugin implementation to register the collection implementation
// it also resisters the supported sources for this collection
// this is required so that the CollectionBase can call the collection's methods
func (b *CollectionBase[T]) RegisterImpl(impl Collection) {
	b.impl = impl
}

// GetSourceOptions give the collection a chance to specify options for the source
// default implementation returning nothing
func (*CollectionBase[T]) GetSourceOptions() []row_source.RowSourceOption {
	return nil
}

// Collect executes the collection process. Tell our source to start collection
func (b *CollectionBase[T]) Collect(ctx context.Context, req *proto.CollectRequest) (json.RawMessage, error) {
	slog.Info("Start collection")

	// create empty status event
	b.status = events.NewStatusEvent(req.ExecutionId)

	// tell our source to collect
	// this is a blocking call, but we will receive and processrow events during the execution
	err := b.Source.Collect(ctx)
	if err != nil {
		return nil, err
	}

	slog.Info("Source collection complete - waiting for enrichment")

	// wait for all rows to be processed
	b.rowWg.Wait()

	// set the end time
	b.enrichTiming.End = time.Now()

	defer slog.Info("Enrichment complete")

	// notify observers of final status
	if err := b.NotifyObservers(ctx, b.status); err != nil {
		slog.Error("Collection RowSourceBase: error notifying observers of status", "error", err)
	}

	// now ask the source for its updated collection state data
	return b.Source.GetCollectionStateJSON()
}

// Notify implements observable.Observer
// it handles all events which collections may receive (these will all come from the source)
func (b *CollectionBase[T]) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	b.updateStatus(ctx, event)

	switch e := event.(type) {
	case *events.Row:
		return b.handleRowEvent(ctx, e)
	case *events.Error:
		return b.handeErrorEvent(e)
	default:
		// ignore
		return nil
	}
}

func (b *CollectionBase[T]) GetTiming() types.TimingCollection {
	return append(b.Source.GetTiming(), b.enrichTiming)
}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (b *CollectionBase[T]) updateStatus(ctx context.Context, e events.Event) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()

	b.status.Update(e)

	// send a status event periodically
	if time.Since(b.lastStatusEventTime) > statusUpdateInterval {
		// notify observers
		if err := b.NotifyObservers(ctx, b.status); err != nil {
			slog.Error("Collection RowSourceBase: error notifying observers of status", "error", err)
		}
		// update lastStatusEventTime
		b.lastStatusEventTime = time.Now()
	}
}

// handleRowEvent is invoked when a Row event is received - enrich the row and publish it
func (b *CollectionBase[T]) handleRowEvent(ctx context.Context, e *events.Row) error {
	b.rowWg.Add(1)
	defer b.rowWg.Done()

	// set the enrich time if not already set
	b.enrichTiming.TryStart(constants.TimingEnrich)

	// when all rows, a null row will be sent - DO NOT try to enrich this!
	row := e.Row
	if row != nil {
		var err error
		enrichStart := time.Now()
		row, err = b.impl.EnrichRow(e.Row, e.EnrichmentFields)
		if err != nil {
			return err
		}

		// update the enrich active duration
		b.enrichTiming.UpdateActiveDuration(time.Since(enrichStart))
	}

	return b.NotifyObservers(ctx, events.NewRowEvent(e.ExecutionId, row, e.CollectionState))
}

func (b *CollectionBase[T]) handeErrorEvent(e *events.Error) error {
	slog.Error("Collection RowSourceBase: error event received", "error", e.Err)
	b.NotifyObservers(context.Background(), e)
	return nil
}
