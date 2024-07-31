package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"log/slog"
	"sync"
)

// Base provides a base implementation of the [plugin.Collection] interface
// it should be embedded in all Collection implementations
type Base[T hcl.Config] struct {
	observable.Base

	// the row Source
	Source row_source.RowSource

	// store a reference to the derived collection type so we can call its methods
	impl plugin.Collection

	// the collection config
	Config T
	// the supported sources for this collection, converted to a lookup
	supportedSourceLookup map[string]struct{}
	rowWg                 sync.WaitGroup
}

// Init implements plugin.Collection
func (b *Base[T]) Init(ctx context.Context, sourceFactory row_source.SourceFactory, collectionConfigData, sourceConfigData *hcl.Data, sourceOpts ...row_source.RowSourceOption) error {
	// parse the config
	c, unknownHcl, err := hcl.ParseConfig[T](collectionConfigData)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	b.Config = c

	slog.Info("Collection Base: config parsed", "config", c, "unknownHcl", string(unknownHcl))
	// TODO #config TEMP - this will actually parse (or the base will)
	// unmarshal the config
	//config := &CloudTrailLogCollectionConfig{
	//	Paths: []string{"/Users/kai/tailpipe_data/flaws_cloudtrail_logs"},
	//}

	// validate config
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	//
	//// todo #config create source from config
	//source, err := b.getSource(sourceConfigData)
	//if err != nil {
	//	return err
	//}
	return b.SetSource(ctx, sourceFactory, sourceConfigData, sourceOpts...)
}

// RegisterImpl is called by the plugin implementation to register the collection implementation
// this is required so that the Base can call the collection's methods
func (b *Base[T]) RegisterImpl(impl plugin.Collection) {
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

// SetSource is called by the plugin implementation to set the row source
func (b *Base[T]) SetSource(ctx context.Context, sourceFactory row_source.SourceFactory, configData *hcl.Data, sourceOpts ...row_source.RowSourceOption) error {
	// first verify we support this source type

	if _, supportsSource := b.supportedSourceLookup[configData.Type]; !supportsSource {
		return fmt.Errorf("source type '%s' is not supported by collection '%s'", configData.Type, b.impl.Identifier())
	}

	// now ask plugin to create and initialise the source for us
	source, err := sourceFactory.GetRowSource(ctx, configData, sourceOpts...)
	if err != nil {
		return err
	}
	b.Source = source

	// add ourselves as an observer to our Source
	return b.Source.AddObserver(b)
}

func (b *Base[T]) Collect(ctx context.Context, req *proto.CollectRequest) (paging.Data, error) {
	slog.Info("Start collection")
	// if the req contains paging data, deserialise it and add to the context passed to the source
	if req.PagingData != nil {
		paging, err := b.getPagingData(req.PagingData)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialise paging data JSON: %w", err)
		}
		ctx = context_values.WithPagingData(ctx, paging)
	}

	// tell our source to collect - we will calls to EnrichRow for each row
	err := b.Source.Collect(ctx)
	if err != nil {
		return nil, err
	}

	slog.Info("Source collection complete - waiting for enrichment")
	// wait for all rows to be processed
	b.rowWg.Wait()

	// ask the source for its paging data
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

// getPagingData deserialises the paging data JSON and returns a paging.Data object
// it uses the impl to return an empty paging.Data object to unmarshal into
func (b *Base[T]) getPagingData(pagingDataJSON json.RawMessage) (paging.Data, error) {
	target, err := b.impl.GetPagingDataSchema()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(pagingDataJSON, target)
	if err != nil {
		return nil, err

	}
	return target, nil
}

func (b *Base[T]) GetPagingDataSchema() (paging.Data, error) {
	return nil, fmt.Errorf("GetPagingDataSchema not implemented by %s", b.impl.Identifier())
}
