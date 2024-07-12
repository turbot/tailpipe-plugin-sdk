package collection

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"log/slog"
	"sync"
)

// Base should be embedded in all tailpipe collection implementations
type Base struct {
	observable.Base

	// the row Source
	Source plugin.RowSource

	enricher plugin.RowEnricher
	rowWg    sync.WaitGroup
}

func (b *Base) Init(enricher plugin.RowEnricher) {
	b.enricher = enricher
}

func (b *Base) AddSource(source plugin.RowSource) {
	b.Source = source

	// add ourselves as an observer to our Source
	b.Source.AddObserver(b)
}

func (b *Base) Collect(ctx context.Context, req *proto.CollectRequest) error {
	slog.Info("Start collection")
	// tell our source to collect - we will calls to EnrichRow for each row
	if err := b.Source.Collect(ctx, req); err != nil {
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
func (b *Base) Notify(event events.Event) error {
	switch e := event.(type) {
	case *events.Row:
		return b.HandleRowEvent(e.Request, e.Row, e.EnrichmentFields)
	default:
		return fmt.Errorf("collection does not handle event type: %T", e)
	}
}

// HandleRowEvent is invoked when a Row event is received - enrich the row and publish it
func (b *Base) HandleRowEvent(req *proto.CollectRequest, row any, sourceEnrichmentFields *enrichment.CommonFields) error {
	// TODO maybe row events should include multiple rows

	b.rowWg.Add(1)
	defer b.rowWg.Done()

	// tell enricher to enrich the row
	// todo #validation move to validate
	if b.enricher == nil {
		// error!
		return fmt.Errorf("no enrich function set")
	}
	enrichedRow, err := b.enricher.EnrichRow(row, sourceEnrichmentFields)
	if err != nil {
		return err
	}

	// row is already enriched - no need to pass enrichment fields
	return b.NotifyObservers(events.NewRowEvent(req, enrichedRow, nil))
}
