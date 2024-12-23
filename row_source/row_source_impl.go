package row_source

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
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
//
// S is the type of the source config struct
// T is the type of the connection struct
type RowSourceImpl[S, T parse.Config] struct {
	observable.ObservableImpl
	Config     S
	Connection T
	// store a reference to the derived RowSource type so we can call its methods
	// this will be set by the source factory
	Source RowSource

	// the collection state data for this source
	CollectionState collection_state.CollectionState[S]
	// a function to create empty collection state data
	NewCollectionStateFunc func() collection_state.CollectionState[S]
}

// RegisterSource is called by the source implementation to register itself with the base
// this is required so that the RowSourceImpl can call the RowSource's methods
func (r *RowSourceImpl[S, T]) RegisterSource(source RowSource) {
	r.Source = source
}

// Init is called when the row source is created
// it is responsible for parsing the source config and configuring the source
func (r *RowSourceImpl[S, T]) Init(ctx context.Context, configData, connectionData types.ConfigData, opts ...RowSourceOption) error {
	slog.Info(fmt.Sprintf("Initializing RowSourceImpl %p, impl %p", r, r.Source))

	// apply options to the Source (as options will be dependent on the outer type)
	for _, opt := range opts {
		if err := opt(r.Source); err != nil {
			return err
		}
	}

	err := r.initialiseConfig(configData)
	if err != nil {
		return err
	}

	err = r.initialiseConnection(connectionData)
	if err != nil {
		return err
	}

	// if no collection state has been se t already (by calling SetCollectionStateJSON) create empty collection state
	// TODO #design is it acceptable to have no collection state? we should put nil checks round access to it
	if r.CollectionState == nil && r.NewCollectionStateFunc != nil {
		slog.Info("Creating empty collection state")
		r.CollectionState = r.NewCollectionStateFunc()
	}

	return nil
}

func (r *RowSourceImpl[S, T]) initialiseConfig(configData types.ConfigData) error {
	// default to empty config
	c := utils.InstanceOf[S]()
	// parse the config
	if len(configData.GetHcl()) > 0 {
		var err error
		c, err = parse.ParseConfig[S](configData)
		if err != nil {
			return err
		}
	}
	// validate config (even if it is empty - this is the config we will be using so it must be valid)
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid source config: %w", err)
	}
	r.Config = c
	return nil
}

func (r *RowSourceImpl[S, T]) initialiseConnection(connectionData types.ConfigData) error {
	// default to empty connection
	conn := utils.InstanceOf[T]()

	if !helpers.IsNil(connectionData) && len(connectionData.GetHcl()) > 0 {
		var err error
		conn, err = parse.ParseConfig[T](connectionData)
		if err != nil {
			return fmt.Errorf("error parsing connection: %w", err)
		}
	}
	r.Connection = conn

	// validate config
	if err := conn.Validate(); err != nil {
		return fmt.Errorf("invalid connection: %w", err)
	}
	return nil
}

// GetConfigSchema returns an empty instance of the config struct used by the source
func (r *RowSourceImpl[S, T]) GetConfigSchema() parse.Config {
	return utils.InstanceOf[T]()
}

// Close is a default implementation of the [plugin.RowSource] Close interface function
func (r *RowSourceImpl[S, T]) Close() error {
	return nil
}

// OnRow raise an [events.Row] event, which is handled by the table.
// It is called by the row source when it has a row to send
func (r *RowSourceImpl[S, T]) OnRow(ctx context.Context, row *types.RowData, collectionState json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	return r.NotifyObservers(ctx, events.NewRowEvent(executionId, row.Data, collectionState, events.WithSourceEnrichment(row.SourceEnrichment)))
}

// GetCollectionStateJSON marshals the collection state data into JSON
func (r *RowSourceImpl[S, T]) GetCollectionStateJSON() (json.RawMessage, error) {
	if r.CollectionState == nil {
		return nil, nil
	}
	mut := r.CollectionState.GetMut()
	mut.RLock()
	defer mut.RUnlock()
	if r.CollectionState.IsEmpty() {
		return nil, nil
	}
	return json.Marshal(r.CollectionState)
}

// SetCollectionStateJSON unmarshalls the collection state data JSON into the target object
func (r *RowSourceImpl[S, T]) SetCollectionStateJSON(collectionStateJSON json.RawMessage) error {
	slog.Info("Setting collection state from JSON", "json", string(collectionStateJSON))

	if len(collectionStateJSON) == 0 {
		return nil
	}
	if r.NewCollectionStateFunc == nil {
		return fmt.Errorf("RowSource implementation must pass CollectionState function to its base to create an empty collection state data struct")
	}

	target := r.NewCollectionStateFunc()
	if err := json.Unmarshal(collectionStateJSON, target); err != nil {
		return err
	}

	r.CollectionState = target
	return nil
}

func (r *RowSourceImpl[S, T]) GetTiming() types.TimingCollection {
	// TODO #observability implement default timing for custom row sourceFuncs
	return types.TimingCollection{}
}

func (*RowSourceImpl[S, T]) Description() string {
	// override if you want to provide a description
	return ""
}
