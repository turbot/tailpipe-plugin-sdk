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

// DefaultAPIGranularity is the default granularity for API sources
const DefaultAPIGranularity = 1 * time.Nanosecond

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
	CollectionState *collection_state.SaveableCollectionState
	// a function to create empty collection state data
	NewCollectionStateFunc func() collection_state.CollectionState
	// store errors - we only use this to determine whether the source collection was successful,
	// and therefore whether we should set the CollectionState EndTime to the collection To time from OnCollectionComplete
	ErrorCount int32

	// the collection direction (this defaults to forwards - reverse order sources should set this in their Init method)
	CollectionOrder collection_state.CollectionOrder
	// the start time for the data collection
	//FromTime time.Time
	// how was from time set (config, collection state, default)
	FromTimeSource      string
	CollectionTimeRange collection_state.DirectionalTimeRange
	// a func to call to retrieve the granularity for the source
	// this is provided to avoid a tricky timing problem - we want to get the granularity within RowSourceImpl.Init
	// but ArtifactSourceImpl.Init  needs to use our config to determine the granularity and this is not available
	// until after the RowSourceImpl.Init is called
	// so ArtifactSourceImpl.Init will set this func to return the granularity
	GetGranularityFunc func() time.Duration
}

// RegisterSource is called by the source implementation to register itself with the base
// this is required so that the RowSourceImpl can call the RowSource's methods
func (r *RowSourceImpl[S, T]) RegisterSource(source RowSource) {
	r.Source = source
}

// Init is called when the row source is created
// it is responsible for parsing the source config and configuring the source
// opts are populated based on the table source config
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

	// create empty collection state and wrap in a SaveableCollectionState
	slog.Info("Creating empty collection state")
	r.CollectionState, err = collection_state.NewSaveableCollectionState(r.NewCollectionStateFunc(), params.CollectionStatePath)
	if err != nil {
		return err
	}

	// if the granularity is not set, default to 1ns (the default for APIs0)
	granularity := DefaultAPIGranularity
	// if the GetGranularityFunc is set, call it to get the granularity
	if r.GetGranularityFunc != nil {
		granularity = r.GetGranularityFunc()
	}

	// populate the collection time range
	// this will resolve to the from time, using the collection state if needed
	// it will also adjust the from and to time if the collection order is reverse
	r.setCollectionTimeRange(params, granularity)

	// After setting the collection time range, trim nil trunk states
	// NOTE: this is done here so that the collection state is clean before we call Init on it.
	// This is important as we may have nil trunk states in the collection state file if the previous
	// collection was done using older version of the code
	if artifactState, ok := r.CollectionState.State.(*collection_state.ArtifactCollectionState); ok {
		artifactState.TrimNilTrunkStates()
	}

	// init the collection state with the time range and granularity
	// NOTE: the collection state will set it;s collection order based on the time range collection order
	// (which we set from our CollectionOrder field)
	err = r.CollectionState.Init(r.CollectionTimeRange, params.Overwrite, granularity)
	if err != nil {
		return err
	}

	return nil
}

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
		Time:   r.CollectionTimeRange.LowerBoundary,
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

// OnCollectionComplete must be called by the source Collect function when the collection is complete
// this updates the end time of the collection state to the collection `to` and saves the collection state
func (r *RowSourceImpl[S, T]) OnCollectionComplete() error {
	if atomic.LoadInt32(&r.ErrorCount) > 0 {
		slog.Info("OnCollectionComplete: Collection completed with errors - NOT setting end time of collection state to collection 'to' time as we may need to recollect some files")
		return nil
	}
	if r.CollectionState == nil {
		slog.Info("OnCollectionComplete: Collection state is nil - not setting end time")
		return nil
	}

	// Before saving, trim nil trunk states
	// Having a null trunk state in the collection state file is valid. There might be some locations in the
	// bucket which have no files in them, which would result in null trunk states, as there is no way to know
	// this in advance we add the null trunk states to the collection state.
	// However, we don't want to save these null trunk states to the collection state file as they do not
	// make sense. So we trim them before saving.
	if artifactState, ok := r.CollectionState.State.(*collection_state.ArtifactCollectionState); ok {
		artifactState.TrimNilTrunkStates()
	}

	// so the source collection was successful, set the end time of the collection state to the collection `to`
	// this ensures that when we run the next collection, we will start from the end time of the previous collection
	if err := r.CollectionState.OnCollectionComplete(); err != nil {
		return fmt.Errorf("error completing collection state: %w", err)
	}

	// save the collection state
	if err := r.CollectionState.Save(); err != nil {
		return fmt.Errorf("error saving collection state: %w", err)
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

func (r *RowSourceImpl[S, T]) setCollectionTimeRange(params *RowSourceParams, granularity time.Duration) {
	// resolve the from time, applying the from time passed in the params
	// and falling back to the collection state/default value if needed
	from, fromSource := r.resolveFromTime(params.From)

	r.CollectionTimeRange = collection_state.DirectionalTimeRange{
		LowerBoundary:   from,
		UpperBoundary:   params.To,
		CollectionOrder: r.CollectionOrder,
	}
	r.FromTimeSource = fromSource

	slog.Info("Collection time range", "from", from, "to", params.To, "order", r.CollectionOrder)
}

// the from time source is the reason why the from time was set - it will be displayed as message on the CLI
var fromTimeSourceCollectionStateEndTime = "collection state end time"
var fromTimeSourceDefault = fmt.Sprintf("initial collection, default %d days", int(constants.DefaultInitialCollectionPeriod.Hours()/24))

// no messaqe for user specified from time - use empty string
var fromTimeSourceUserSpecified = ""

// SetFromTime sets the from time for the data collection
// If the from time is not set, it will be set to the end time of the collection state
// If the collection state is empty, it will be set to the default initial collection period
func (r *RowSourceImpl[S, T]) resolveFromTime(from time.Time) (fromTime time.Time, fromTimeSource string) {
	if !from.IsZero() {
		// a from time pass passed as a pram - use it
		return from, fromTimeSourceUserSpecified
	}
	// if no from time was passed, set it to the end time of the collection state
	if !r.CollectionState.IsEmpty() {
		t := r.CollectionState.GetToTime()
		if !t.IsZero() {
			slog.Info("Setting from time from collection state end time", "end time", t)
			return t, fromTimeSourceCollectionStateEndTime
		}
	}

	slog.Info("Setting from time to default", "default", constants.DefaultInitialCollectionPeriod)

	// if from is not set (either by explicitly passing is as an arg, or from the collection state end time) set it now
	// to the default (7 days
	fromTime = time.Now().Add(-constants.DefaultInitialCollectionPeriod)
	return fromTime, fromTimeSourceDefault
}
