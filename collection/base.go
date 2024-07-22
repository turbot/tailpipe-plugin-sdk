package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"log/slog"
	"sync"
)

// Base should be embedded in all tailpipe collection implementations
type Base struct {
	observable.Base

	// the row Source
	Source plugin.RowSource

	// store a reference to the derived collection type so we can call its methods
	impl plugin.Collection

	rowWg sync.WaitGroup
}

func (b *Base) RegisterImpl(impl plugin.Collection) {
	b.impl = impl
}

func (b *Base) AddSource(source plugin.RowSource) error {
	b.Source = source

	// add ourselves as an observer to our Source
	return b.Source.AddObserver(b)
}

func (b *Base) Collect(ctx context.Context, req *proto.CollectRequest) error {
	slog.Info("Start collection")
	// if the req contains paging data, deserialise it and add to the context passed to the source
	if req.PagingData != nil {
		paging, err := b.impl.GetPagingData(req.PagingData)
		if err != nil {
			return fmt.Errorf("failed to deserialise paging data JSON: %w", err)
		}
		ctx = context_values.WithPagingData(ctx, paging)
	}

	// tell our source to collect - we will calls to EnrichRow for each row
	err := b.Source.Collect(ctx)
	if err != nil {
		return err
	}

	slog.Info("Source collection complete - waiting for enrichment")
	// wait for all rows to be processed
	b.rowWg.Wait()

	slog.Info("Enrichment complete")
	return nil
}

// Notify implements observable.Observer
// it handles all events which collections may receive (these will all come from the source)
func (b *Base) Notify(ctx context.Context, event events.Event) error {
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
func (b *Base) handleRowEvent(ctx context.Context, e *events.Row) error {
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

func (b *Base) handeErrorEvent(e *events.Error) error {
	// todo #err how to bubble up error
	slog.Error("Collection Base: error event received", "error", e.Err)
	return nil
}

// GetPagingData deserialises the paging data JSON and returns a paging.Data object
// it uses the impl to return an empty paging.Data object to unmarshal into
func (b *Base) GetPagingData(pagingDataJSON json.RawMessage) (paging.Data, error) {
	empty, err := b.impl.NewPagingData()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(pagingDataJSON, empty)
	if err != nil {
		return nil, err

	}
	return empty, nil
}
func (b *Base) NewPagingData() (paging.Data, error) {
	return nil, fmt.Errorf("NewPagingData not implemented by %s", b.impl.Identifier())
}
