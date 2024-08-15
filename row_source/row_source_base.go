package row_source

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// RowSourceBase is a base implementation of the [plugin.RowSource] interface
// It implements the [observable.Observable] interface, as well as providing a default implementation of
// Close(), and contains the logic to raise a Row event
// It should be embedded in all [plugin.RowSource] implementations
type RowSourceBase[T hcl.Config] struct {
	observable.ObservableBase
	Config T
	// store a reference to the derived RowSource type so we can call its methods
	Impl RowSource

	// the paging data for this source
	PagingData paging.Data
}

// Init is called when the row source is created
// it is responsible for parsing the source config and configuring the source

// RegisterImpl is called by the plugin implementation to register the collection implementation
// this is required so that the RowSourceBase can call the RowSource's methods
func (b *RowSourceBase[T]) RegisterImpl(impl RowSource) {
	b.Impl = impl
}

func (b *RowSourceBase[T]) Init(ctx context.Context, configData *hcl.Data, opts ...RowSourceOption) error {
	// apply options to the Impl (as options will be dependent on the outer type)
	for _, opt := range opts {
		opt(b.Impl)
	}

	// parse the config
	var emptyConfig T = b.Impl.GetConfigSchema().(T)
	c, err := hcl.ParseConfig[T](configData, emptyConfig)
	if err != nil {
		return err
	}

	// validate config
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	b.Config = c
	return nil
}

// Close is a default implementation of the [plugin.RowSource] Close interface function
func (b *RowSourceBase[T]) Close() error {
	return nil
}

// OnRow raise an [events.Row] event, which is handled by the collection.
// It is called by the row source when it has a row to send
func (b *RowSourceBase[T]) OnRow(ctx context.Context, row *types.RowData, pagingData json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	return b.NotifyObservers(ctx, events.NewRowEvent(executionId, row.Data, pagingData, events.WithEnrichmentFields(row.Metadata)))
}

// GetPagingDataSchema should be overriden by the RowSource implementation to return the paging data schema
// base implementation returns nil
func (b *RowSourceBase[T]) GetPagingDataSchema() paging.Data {
	return nil
}

// GetPagingData returns the current paging data for the ongoing collection
func (b *RowSourceBase[T]) GetPagingData() (json.RawMessage, error) {
	if b.PagingData == nil {
		return nil, nil
	}
	mut := b.PagingData.GetMut()
	mut.RLock()
	defer mut.RUnlock()
	return json.Marshal(b.PagingData)
}

// SetPagingData unmarshalls the paging data JSON into the target object
func (b *RowSourceBase[T]) SetPagingData(pagingDataJSON json.RawMessage) error {
	target := b.Impl.GetPagingDataSchema()
	if target == nil {
		return fmt.Errorf("GetPagingDataSchema must be implemented by the %s RowSource", b.Impl.Identifier())
	}

	if err := json.Unmarshal(pagingDataJSON, target); err != nil {
		return err
	}

	b.PagingData = target
	return nil
}

func (b *RowSourceBase[T]) GetTiming() types.TimingMap {
	// TODO #observability implement default timing for custom row sources
	return types.TimingMap{}
}
