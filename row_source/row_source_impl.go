package row_source

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/turbot/go-kit/helpers"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/hclhelpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
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
	observable.PausableObservableImpl
	Config     S
	Connection T
	// store a reference to the derived RowSource type so we can call its methods
	// this will be set by the source factory
	Source RowSource

	// the collection state data for this source
	CollectionState collection_state.CollectionState[S]
	// a function to create empty collection state data
	NewCollectionStateFunc func() collection_state.CollectionState[S]
	// the start time for the data collection
	FromTime time.Time
	// how was from time set (config, collection state, default)
	FromTimeSource string
	// the end time for the data collection
	ToTime time.Time
	// store errors - we only use this to determine whether the source collection was successful,
	// and therefore whether we should set the CollectionState EndTime to the collection To time from OnCollectionComplete
	ErrorCount int32
}

// RegisterSource is called by the source implementation to register itself with the base
// this is required so that the RowSourceImpl can call the RowSource's methods
func (r *RowSourceImpl[S, T]) RegisterSource(source RowSource) {
	r.Source = source
}

// Init is called when the row source is created
// it is responsible for parsing the source config and configuring the source
func (r *RowSourceImpl[S, T]) Init(_ context.Context, params *RowSourceParams, opts ...RowSourceOption) error {
	slog.Info(fmt.Sprintf("Initializing RowSourceImpl %p, impl %p", r, r.Source))
	if r.NewCollectionStateFunc == nil {
		return fmt.Errorf("NewCollectionStateFunc not set")
	}
	// apply options to the Source (as options will be dependent on the outer type)
	for _, opt := range opts {
		if err := opt(r.Source); err != nil {
			return err
		}
	}

	err := r.initialiseConfig(params.SourceConfigData)
	if err != nil {
		return err
	}

	err = r.initialiseConnection(params.ConnectionData)
	if err != nil {
		return err
	}

	// create empty collection state
	slog.Info("Creating empty collection state")
	r.CollectionState = r.NewCollectionStateFunc()
	// initialise the collection state - this will load itself form json (if JSON file exists)
	err = r.CollectionState.Init(r.Config, params.CollectionStatePath)
	if err != nil {
		return err
	}
	// populate the from time, applying the from time passed in the params
	// and falling back to the collection state/default value if needed
	r.setFromTime(params)
	r.ToTime = params.To

	return nil
}

// TODO who needs this
func (r *RowSourceImpl[S, T]) SaveCollectionState() error {
	return r.CollectionState.Save()
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
func (r *RowSourceImpl[S, T]) OnRow(ctx context.Context, row *types.RowData) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	return r.NotifyObservers(ctx, events.NewRowExtractedEvent(executionId, row.Data, *row.SourceEnrichment))
}

// GetFromTime returns the start time for the data collection, including the source of the from time
// (config, collection state or default)
func (r *RowSourceImpl[S, T]) GetFromTime() *ResolvedFromTime {
	return &ResolvedFromTime{
		Time:   r.FromTime,
		Source: r.FromTimeSource,
	}
}

// Description returns a human readable description of the source
// this is used for introspection
// this should be overridden by the source implementation
func (*RowSourceImpl[S, T]) Description() (string, error) {
	// override if you want to provide a description
	return "", nil
}

// Properties returns a map of property descriptions
// this is used for introspection
// this should be overridden by the source implementation
func (r *RowSourceImpl[S, T]) Properties() map[string]*types.PropertyMetadata {
	return r.PropertiesForType(utils.InstanceOf[S]())
}

func (r *RowSourceImpl[S, T]) PropertiesForType(config any) map[string]*types.PropertyMetadata {
	properties := make(map[string]*types.PropertyMetadata)
	configType := reflect.TypeOf(config)
	if configType.Kind() == reflect.Ptr {
		configType = configType.Elem()
	}

	for i := 0; i < configType.NumField(); i++ {
		field := configType.Field(i)
		if hclTagStr := field.Tag.Get("hcl"); hclTagStr != "" {
			hclTag, err := hclhelpers.NewHclTag(hclTagStr)
			if err != nil {
				slog.Error("error parsing hcl tag", "tag", hclTagStr, "error", err)
				continue
			}
			// if this is the remain field, ignore
			if hclTag.Remain {
				continue
			}

			// field is optional if it's a nullable type or pointer or if optional tag is set
			isOptional := field.Type.Kind() == reflect.Ptr ||
				field.Type.Kind() == reflect.Struct ||
				field.Type.Kind() == reflect.Map ||
				field.Type.Kind() == reflect.Slice ||
				typehelpers.BoolValue(hclTag.Optional)

			properties[hclTag.Tag] = &types.PropertyMetadata{
				// remove leading * from type
				Type:     strings.TrimPrefix(field.Type.String(), "*"),
				Required: !isOptional,
			}
		}
	}
	return properties
}

func (r *RowSourceImpl[S, T]) OnCollectionStarted() error {
	return r.CollectionState.OnCollectionStarted(r.FromTime, r.ToTime)
}

// OnCollectionComplete must be called by the source Collect function when the collection is complete
// this updates the end time of the collection state to the collection `To` and saves the collection state
func (r *RowSourceImpl[S, T]) OnCollectionComplete() error {
	if atomic.LoadInt32(&r.ErrorCount) > 0 {
		slog.Info("OnCollectionComplete: Collection completed with errors - NOT setting end time of collection state to collection 'to' time as we may need to recollect some files")
		return nil
	}
	if r.CollectionState == nil {
		slog.Info("OnCollectionComplete: Collection state is nil - not setting end time")
		return nil
	}
	// so the source collection was successful, set the end time of the collection state to the collection `To`
	// this ensures that when we run the next collection, we will start from the end time of the previous collection
	r.CollectionState.OnCollectionComplete()

	// save the collection state
	if err := r.CollectionState.Save(); err != nil {
		return fmt.Errorf("error saving collection state: %w", err)
	}
	return nil
}

func (r *RowSourceImpl[S, T]) setFromTime(params *RowSourceParams) {
	if !params.From.IsZero() {
		// just set the collection state end time
		r.FromTime = params.From
		r.FromTimeSource = ""
		return
	}
	// if no from time was passed, set it to the end time of the collection state
	if !r.CollectionState.IsEmpty() {
		t := r.CollectionState.GetToTime()
		if !t.IsZero() {
			slog.Info("Setting from time from collection state end time", "end time", t)
			r.FromTime = t
			r.FromTimeSource = "last collection date"
			return
		}
	}

	slog.Info("Setting from time to default", "default", constants.DefaultInitialCollectionPeriod)

	// if from is not set (either by explicitly passing is as an arg, or from the collection state end time) set it now
	// to the default (7 days
	r.FromTime = time.Now().Add(-constants.DefaultInitialCollectionPeriod)
	r.FromTimeSource = fmt.Sprintf("initial collection, default %d days", int(constants.DefaultInitialCollectionPeriod.Hours()/24))
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
