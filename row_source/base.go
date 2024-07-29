package row_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"log/slog"
)

// Base is a base implementation of the [plugin.RowSource] interface
// It implements the [observable.Observable] interface, as well as providing a default implementation of
// Close(), and contains the logic to raise a Row event
// It should be embedded in all [plugin.RowSource] implementations
type Base[T hcl.Config] struct {
	observable.Base
	Config T
}

// Init is called when the row source is created
// it is responsible for parsing the source config and configuring the source
func (b *Base[T]) Init(ctx context.Context, configData *hcl.Data, opts ...RowSourceOption) error {
	// apply options
	for _, opt := range opts {
		opt(b)
	}

	// parse the c
	c, unknownHcl, err := hcl.ParseConfig[T](configData)
	if err != nil {
		return err
	}

	slog.Info("row_source Base: c parsed", "c", c, "unknownHcl", string(unknownHcl))
	b.Config = c
	return nil
}

// Close is a default implementation of the [plugin.RowSource] Close interface function
func (b *Base[T]) Close() error {
	return nil
}

// OnRow raise an [events.Row] event, which is handled by the collection.
// It is called by the row source when it has a row to send
func (b *Base[T]) OnRow(ctx context.Context, row *artifact.ArtifactData, pagingData paging.Data) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	return b.NotifyObservers(ctx, events.NewRowEvent(executionId, row.Data, pagingData, events.WithEnrichmentFields(row.Metadata)))
}
