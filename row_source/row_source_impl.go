package row_source

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// RowSourceImpl is a base implementation of the [plugin.RowSource] interface
// It implements the [observable.Observable] interface, as well as providing a default implementation of
// Close(), and contains the logic to raise a Row event
// It should be embedded in all [plugin.RowSource] implementations
type RowSourceImpl[T parse.Config] struct {
	observable.ObservableImpl
	Config T
	// store a reference to the derived RowSource type so we can call its methods
	Source RowSource

	// the collection state data for this source
	CollectionState collection_state.CollectionState[T]
	// a function to create empty collection state data
	NewCollectionStateFunc func() collection_state.CollectionState[T]
}

// RegisterSource is called by the source implementation to register itself with the base
// this is required so that the RowSourceImpl can call the RowSource's methods
func (b *RowSourceImpl[T]) RegisterSource(source RowSource) {
	b.Source = source
}

// Init is called when the row source is created
// it is responsible for parsing the source config and configuring the source
func (b *RowSourceImpl[T]) Init(ctx context.Context, configData config_data.ConfigData, opts ...RowSourceOption) error {
	slog.Info(fmt.Sprintf("Initializing RowSourceImpl %p, impl %p", b, b.Source))

	// apply options to the Source (as options will be dependent on the outer type)
	for _, opt := range opts {
		if err := opt(b.Source); err != nil {
			return err
		}
	}

	// parse the config
	if len(configData.GetHcl()) > 0 {
		var emptyConfig T = b.Source.GetConfigSchema().(T)
		c, err := parse.ParseConfig[T](configData, emptyConfig)
		if err != nil {
			return err
		}
		// validate config
		if err := c.Validate(); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}
		b.Config = c
	}

	// if no collection state has been se t already (by calling SetCollectionStateJSON) create empty collection state
	// TODO #design is it acceptable to have no collection state? we should put nil checks round access to it
	if b.CollectionState == nil && b.NewCollectionStateFunc != nil {
		slog.Info("Creating empty collection state")
		b.CollectionState = b.NewCollectionStateFunc()
	}

	return nil
}

// GetConfigSchema returns an empty instance of the config struct used by the source
func (b *RowSourceImpl[T]) GetConfigSchema() parse.Config {
	return utils.InstanceOf[T]()
}

// Close is a default implementation of the [plugin.RowSource] Close interface function
func (b *RowSourceImpl[T]) Close() error {
	return nil
}

// OnRow raise an [events.Row] event, which is handled by the table.
// It is called by the row source when it has a row to send
func (b *RowSourceImpl[T]) OnRow(ctx context.Context, row *types.RowData, collectionState json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	return b.NotifyObservers(ctx, events.NewRowEvent(executionId, row.Data, collectionState, events.WithEnrichmentFields(row.Metadata)))
}

// GetCollectionStateJSON marshals the collection state data into JSON
func (b *RowSourceImpl[T]) GetCollectionStateJSON() (json.RawMessage, error) {
	if b.CollectionState == nil {
		return nil, nil
	}
	mut := b.CollectionState.GetMut()
	mut.RLock()
	defer mut.RUnlock()
	if b.CollectionState.IsEmpty() {
		return nil, nil
	}
	return json.Marshal(b.CollectionState)
}

// SetCollectionStateJSON unmarshalls the collection state data JSON into the target object
func (b *RowSourceImpl[T]) SetCollectionStateJSON(collectionStateJSON json.RawMessage) error {
	slog.Info("Setting collection state from JSON", "json", string(collectionStateJSON))

	if len(collectionStateJSON) == 0 {
		return nil
	}
	if b.NewCollectionStateFunc == nil {
		return fmt.Errorf("RowSource implementation must pass CollectionState function to its base to create an empty collection state data struct")
	}

	target := b.NewCollectionStateFunc()
	if err := json.Unmarshal(collectionStateJSON, target); err != nil {
		return err
	}

	b.CollectionState = target
	return nil
}

func (b *RowSourceImpl[T]) GetTiming() types.TimingCollection {
	// TODO #observability implement default timing for custom row sourceFuncs
	return types.TimingCollection{}
}
