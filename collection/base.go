package collection

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

// Base provides a base implementation of the [plugin.Collection] interface
// it should be embedded in all Collection implementations
type Base[T hcl.Config] struct {
	observable.Base

	// the row Source
	Source row_source.RowSource

	// store a reference to the derived collection type so we can call its methods
	impl Collection

	// the collection config
	Config T
	// the supported sources for this collection, converted to a lookup
	supportedSourceLookup map[string]struct{}
	rowWg                 sync.WaitGroup
}

// Init implements plugin.Collection
func (b *Base[T]) Init(ctx context.Context, collectionConfigData, sourceConfigData *hcl.Data, sourceOpts ...row_source.RowSourceOption) error {
	// parse the config
	c, unknownHcl, err := hcl.ParseConfig[T](collectionConfigData)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	b.Config = c

	slog.Info("Collection Base: config parsed", "config", c, "unknownHcl", string(unknownHcl))

	// validate config
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	return b.initSource(ctx, sourceConfigData, sourceOpts...)
}

// initialise the row source
func (b *Base[T]) initSource(ctx context.Context, configData *hcl.Data, sourceOpts ...row_source.RowSourceOption) error {
	// first verify we support this source type
	if _, supportsSource := b.supportedSourceLookup[configData.Type]; !supportsSource {
		return fmt.Errorf("source type '%s' is not supported by collection '%s'", configData.Type, b.impl.Identifier())
	}

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
// this is required so that the Base can call the collection's methods
func (b *Base[T]) RegisterImpl(impl Collection) {
	b.impl = impl
	b.supportedSourceLookup = utils.SliceToLookup(impl.SupportedSources())
}

// GetConfigSchema implements plugin.Collection
func (b *Base[T]) GetConfigSchema() any {
	var emptyConfig T
	return emptyConfig
}

func (*Base[T]) GetSourceOptions(sourceType string) []row_source.RowSourceOption {
	return nil
}

func (b *Base[T]) Collect(ctx context.Context, req *proto.CollectRequest) (paging.Data, error) {
	slog.Info("Start collection")
	// if the req contains paging data, tell the source to deserialize and store it
	if req.PagingData != nil {
		// ask the source to deserialise the paging data
		if err := b.Source.SetPagingData(req.PagingData); err != nil {
			return nil, fmt.Errorf("failed to deserialise paging data JSON: %w", err)
		}
	}

	// tell our source to collect - we will calls to EnrichRow for each row
	err := b.Source.Collect(ctx)
	if err != nil {
		return nil, err
	}

	slog.Info("Source collection complete - waiting for enrichment")
	// wait for all rows to be processed
	b.rowWg.Wait()

	// now ask the source for its updated paging data
	pagingData := b.Source.GetPagingData()

	slog.Info("Enrichment complete")
	return pagingData, nil
}

// Notify implements observable.Observer
// it handles all events which collections may receive (these will all come from the source)
func (b *Base[T]) Notify(ctx context.Context, event events.Event) error {
	switch e := event.(type) {
	case *events.Row:
		return b.handleRowEvent(ctx, e)
		// error
	case *events.Error:
		return b.handeErrorEvent(e)
	default:
		return fmt.Errorf("collection does not handle event type: %T", e)
	}
}

// handleRowEvent is invoked when a Row event is received - enrich the row and publish it
func (b *Base[T]) handleRowEvent(ctx context.Context, e *events.Row) error {
	b.rowWg.Add(1)
	defer b.rowWg.Done()

	// when all rows, a null row will be sent - DO NOT try to enrich this!
	row := e.Row

	if row != nil {
		var err error
		row, err = b.impl.EnrichRow(e.Row, e.EnrichmentFields)
		if err != nil {
			return err
		}
	}

	return b.NotifyObservers(ctx, events.NewRowEvent(e.ExecutionId, row, e.PagingData))
}

func (b *Base[T]) handeErrorEvent(e *events.Error) error {
	// todo #err how to bubble up error
	slog.Error("Collection Base: error event received", "error", e.Err)
	return nil
}
