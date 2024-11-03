package table

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
	"log/slog"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// how ofted to send status events
const statusUpdateInterval = 250 * time.Millisecond

// TableImpl provides a base implementation of the [table.Table] interface
// it should be embedded in all Table implementations
// R is the type of the row struct
// S is the type table config struct
// T is the type of the connection
type TableImpl[R any, S, T parse.Config] struct {
	observable.ObservableImpl

	// the row Source
	Source row_source.RowSource

	// store a reference to the actual table (via the generic Enricher interface) so we can call its methods
	table Enricher[R]

	// the table config
	Config S
	// the connection config
	Connection T

	// row mappers
	Mappers []artifact_mapper.Mapper

	// wait group to wait for all rows to be processed
	// this is incremented each time we receive a row event and decremented when we have processed it
	rowWg sync.WaitGroup

	// we only send status events periodically, to avoid flooding the event stream
	// store a status event and we will update it each time we receive artifact or row events
	status              *events.Status
	lastStatusEventTime time.Time
	statusLock          sync.RWMutex

	enrichTiming types.Timing
}

// Init implements table.Table
func (b *TableImpl[R, S, T]) Init(ctx context.Context, connectionSchemaProvider ConnectionSchemaProvider, req *types.CollectRequest) error {
	if err := b.initialiseConfig(req.TableData); err != nil {
		return err
	}

	if err := b.initialiseConnection(connectionSchemaProvider, req.ConnectionData); err != nil {
		return err
	}

	// initialise the source
	sourceOpts := b.table.GetSourceOptions(req.SourceData.Type)
	// if collectionStateJSON is non-empty, add an option to set it
	if len(req.CollectionState) > 0 {
		sourceOpts = append(sourceOpts, row_source.WithCollectionStateJSON(req.CollectionState))
	}

	if err := b.initSource(ctx, req.SourceData, sourceOpts...); err != nil {
		return err
	}

	return nil
}

func (b *TableImpl[R, S, T]) initialiseConfig(tableConfigData *types.ConfigData) error {
	if len(tableConfigData.Hcl) > 0 {
		// parse the config
		var emptyConfig = b.table.GetConfigSchema().(S)
		c, err := parse.ParseConfig[S](tableConfigData, emptyConfig)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}
		b.Config = c

		slog.Info("Table RowSourceImpl: config parsed", "config", c)

		// validate config
		if err := c.Validate(); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}
	}
	return nil
}

func (b *TableImpl[R, S, T]) initialiseConnection(connectionSchemaProvider ConnectionSchemaProvider, connectionData *types.ConfigData) error {
	if len(connectionData.Hcl) > 0 {
		// parse the config
		var emptyConfig, ok = connectionSchemaProvider.GetConnectionSchema().(T)
		if !ok {
			return fmt.Errorf("connection schema provider does not return the correct type")
		}
		c, err := parse.ParseConfig[T](connectionData, emptyConfig)
		if err != nil {
			return fmt.Errorf("error parsing connection: %w", err)
		}
		b.Connection = c

		slog.Info("Table RowSourceImpl: } parsed", "}", c)

		// validate config
		if err := c.Validate(); err != nil {
			return fmt.Errorf("invalid }: %w", err)
		}
	}
	return nil
}

// initialise the row source
func (b *TableImpl[R, S, T]) initSource(ctx context.Context, configData *types.ConfigData, sourceOpts ...row_source.RowSourceOption) error {
	// TODO verify we support this source type https://github.com/turbot/tailpipe-plugin-sdk/issues/16

	// now ask plugin to create and initialise the source for us
	source, err := row_source.Factory.GetRowSource(ctx, configData, sourceOpts...)
	if err != nil {
		return err
	}
	b.Source = source

	// add ourselves as an observer to our Source
	return b.Source.AddObserver(b)
}

// RegisterImpl is called by the plugin implementation to register the collection implementation
// it also resisters the supported sources for this collection
// this is required so that the TableImpl can call the collection's methods
func (b *TableImpl[R, S, T]) RegisterImpl(impl Table) {
	// we expect the table to be a Enricher
	b.table = impl.(Enricher[R])
}

// GetSourceOptions give the collection a chance to specify options for the source
// default implementation returning nothing
func (*TableImpl[R, S, T]) GetSourceOptions(sourceType string) []row_source.RowSourceOption {
	return nil
}

// Collect executes the collection process. Tell our source to start collection
func (b *TableImpl[R, S, T]) Collect(ctx context.Context, req *types.CollectRequest) (json.RawMessage, error) {
	slog.Info("Start collection")

	// create empty status event
	b.status = events.NewStatusEvent(req.ExecutionId)

	// tell our source to collect
	// this is a blocking call, but we will receive and processrow events during the execution
	err := b.Source.Collect(ctx)
	if err != nil {
		return nil, err
	}

	slog.Info("Source collection complete - waiting for enrichment")

	// wait for all rows to be processed
	b.rowWg.Wait()

	// set the end time
	b.enrichTiming.End = time.Now()

	defer slog.Info("Enrichment complete")

	// notify observers of final status
	if err := b.NotifyObservers(ctx, b.status); err != nil {
		slog.Error("Table RowSourceImpl: error notifying observers of status", "error", err)
	}

	// now ask the source for its updated collection state data
	return b.Source.GetCollectionStateJSON()
}

// Notify implements observable.Observer
// it handles all events which tableFuncs may receive (these will all come from the source)
func (b *TableImpl[R, S, T]) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	b.updateStatus(ctx, event)

	switch e := event.(type) {
	case *events.Row:
		return b.handleRowEvent(ctx, e)
	case *events.Error:
		return b.handeErrorEvent(e)
	default:
		// ignore
		return nil
	}
}

func (b *TableImpl[R, S, T]) GetTiming() types.TimingCollection {
	return append(b.Source.GetTiming(), b.enrichTiming)
}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (b *TableImpl[R, S, T]) updateStatus(ctx context.Context, e events.Event) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()

	b.status.Update(e)

	// send a status event periodically
	if time.Since(b.lastStatusEventTime) > statusUpdateInterval {
		// notify observers
		if err := b.NotifyObservers(ctx, b.status); err != nil {
			slog.Error("Table RowSourceImpl: error notifying observers of status", "error", err)
		}
		// update lastStatusEventTime
		b.lastStatusEventTime = time.Now()
	}
}

// handleRowEvent is invoked when a Row event is received - map, enrich and publish the row
func (b *TableImpl[R, S, T]) handleRowEvent(ctx context.Context, e *events.Row) error {
	b.rowWg.Add(1)
	defer b.rowWg.Done()

	// when all rows, a null row will be sent - DO NOT try to enrich this!
	row := e.Row
	if row == nil {
		// notify of nil row
		return b.NotifyObservers(ctx, events.NewRowEvent(e.ExecutionId, row, e.CollectionState))
	}

	// put data into an array as that is what mappers expect
	rows, err := b.mapRow(ctx, row)
	if err != nil {
		return fmt.Errorf("error mapping artifact: %w", err)
	}

	// set the enrich time if not already set
	b.enrichTiming.TryStart(constants.TimingEnrich)

	enrichStart := time.Now()
	for _, mappedRow := range rows {
		// convert row to the correct type - if it is the wrong type this is a programming error
		record, ok := mappedRow.(R)
		if !ok {
			return fmt.Errorf("mapper returned invalid row type: %T, expected %T", row, record)
		}

		// enrich the row
		enrichedRow, err := b.table.EnrichRow(record, e.EnrichmentFields)
		if err != nil {
			return err
		}

		if err := b.NotifyObservers(ctx, events.NewRowEvent(e.ExecutionId, enrichedRow, e.CollectionState)); err != nil {
			return err
		}

	}
	// update the enrich active duration
	b.enrichTiming.UpdateActiveDuration(time.Since(enrichStart))

	return nil
}

func (b *TableImpl[R, S, T]) handeErrorEvent(e *events.Error) error {
	slog.Error("Table RowSourceImpl: error event received", "error", e.Err)
	b.NotifyObservers(context.Background(), e)
	return nil
}

// mapROw applies any configured mappers to the artifact data
func (b *TableImpl[R, S, T]) mapRow(ctx context.Context, rawRow any) ([]any, error) {
	// mappers may return multiple rows so wrap data in a list
	var dataList = []any{rawRow}

	// iff there is no mappers, just return the data as is
	if len(b.Mappers) == 0 {
		return dataList, nil
	}

	var errList []error

	// invoke each mapper in turn
	for _, m := range b.Mappers {
		var mappedDataList []any
		for _, d := range dataList {
			mappedData, err := m.Map(ctx, d)
			if err != nil {
				// TODO #error should we give up immediately
				errList = append(errList, err)
			} else {
				mappedDataList = append(mappedDataList, mappedData...)
			}
		}

		// update artifactData list
		dataList = mappedDataList
	}

	if len(errList) > 0 {
		return nil, fmt.Errorf("error mapping artifact rows: %w", errors.Join(errList...))
	}

	return dataList, nil
}
