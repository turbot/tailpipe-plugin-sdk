package row_source

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// RowSourceBase is a base implementation of the [plugin.RowSource] interface
// It implements the [observable.Observable] interface, as well as providing a default implementation of
// Close(), and contains the logic to raise a Row event
// It should be embedded in all [plugin.RowSource] implementations
type RowSourceBase[T parse.Config] struct {
	observable.ObservableBase
	Config T
	// store a reference to the derived RowSource type so we can call its methods
	Impl RowSource

	// the collection state data for this source
	CollectionState collection_state.CollectionState
	// a function to create empty collection state data
	newCollectionStateFunc func(...collection_state.CollectStateOption) collection_state.CollectionState
	collectionStateOpts    []collection_state.CollectStateOption
}

// RegisterImpl is called by the plugin implementation to register the collection implementation
// this is required so that the RowSourceBase can call the RowSource's methods
func (b *RowSourceBase[T]) RegisterImpl(impl RowSource) {
	b.Impl = impl
}

// Init is called when the row source is created
// it is responsible for parsing the source config and configuring the source
func (b *RowSourceBase[T]) Init(ctx context.Context, configData *parse.Data, opts ...RowSourceOption) error {
	slog.Info(fmt.Sprintf("Initializing RowSourceBase %p, impl %p", b, b.Impl))

	// apply options to the Impl (as options will be dependent on the outer type)
	for _, opt := range opts {
		if err := opt(b.Impl); err != nil {
			return err
		}
	}

	// parse the config
	if len(configData.Hcl) > 0 {
		var emptyConfig T = b.Impl.GetConfigSchema().(T)
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

	// if no collection state has been set already (by calling SetCollectionStateJSON) create empty collection state
	// TODO #design is it acceptable to have no collection state? we should put nil checks round access to it
	if b.CollectionState == nil && b.newCollectionStateFunc != nil {
		slog.Info("Creating empty collection state")
		b.CollectionState = b.newCollectionStateFunc(b.collectionStateOpts...)
	}

	return nil
}

// Close is a default implementation of the [plugin.RowSource] Close interface function
func (b *RowSourceBase[T]) Close() error {
	return nil
}

// OnRow raise an [events.Row] event, which is handled by the collection.
// It is called by the row source when it has a row to send
func (b *RowSourceBase[T]) OnRow(ctx context.Context, row *types.RowData, collectionState json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	return b.NotifyObservers(ctx, events.NewRowEvent(executionId, row.Data, collectionState, events.WithEnrichmentFields(row.Metadata)))
}

func (b *RowSourceBase[T]) SetCollectionStateFunc(f func(...collection_state.CollectStateOption) collection_state.CollectionState) {
	b.newCollectionStateFunc = f
}

func (b *RowSourceBase[T]) SetCollectionStateOpts(opts ...collection_state.CollectStateOption) {
	b.collectionStateOpts = opts
}

// GetCollectionStateJSON marshals the collection state data into JSON
func (b *RowSourceBase[T]) GetCollectionStateJSON() (json.RawMessage, error) {
	if b.CollectionState == nil {
		return nil, nil
	}
	mut := b.CollectionState.GetMut()
	mut.RLock()
	defer mut.RUnlock()
	return json.Marshal(b.CollectionState)
}

// SetCollectionStateJSON unmarshalls the collection state data JSON into the target object
func (b *RowSourceBase[T]) SetCollectionStateJSON(collectionStateJSON json.RawMessage) error {
	slog.Info("Setting collection state from JSON", "json", string(collectionStateJSON))

	if len(collectionStateJSON) == 0 {
		return nil
	}
	if b.newCollectionStateFunc == nil {
		return fmt.Errorf("RowSource implementation must pass CollectionState function to its base to create an empty collection state data struct")
	}

	target := b.newCollectionStateFunc()
	if err := json.Unmarshal(collectionStateJSON, target); err != nil {
		return err
	}

	b.CollectionState = target
	return nil
}

func (b *RowSourceBase[T]) GetTiming() types.TimingCollection {
	// TODO #observability implement default timing for custom row sources
	return types.TimingCollection{}
}
