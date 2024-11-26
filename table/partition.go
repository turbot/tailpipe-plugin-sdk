package table

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"log/slog"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// how often to send status events

const statusUpdateInterval = 250 * time.Millisecond

// Partition is a generic implementaion of the Collector interface
// it is responsible for coordinating the collection process and reporting status
// R is the type of the row struct
// S is the type of the partition config
// T is the type of the table
// U is the type of the connection
type Partition[R types.RowStruct, S parse.Config, T Table[R, S]] struct {
	observable.ObservableImpl

	table  Table[R, S]
	source row_source.RowSource
	mapper Mapper[R]

	// the table config
	Config S

	// wait group to wait for all rows to be processed
	// this is incremented each time we receive a row event and decremented when we have processed it
	rowWg               sync.WaitGroup
	status              *events.Status
	lastStatusEventTime time.Time

	statusLock   sync.RWMutex
	enrichTiming types.Timing
	req          *types.CollectRequest
}

func (c *Partition[R, S, T]) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req
	// parse partition config
	if err := c.initialiseConfig(req.PartitionData); err != nil {
		return err
	}

	slog.Info("Table RowSourceImpl: Collect", "table", c.table.Identifier())
	if err := c.initSource(ctx, req.SourceData, req.ConnectionData); err != nil {
		return err
	}
	slog.Info("Start collection")

	return nil
}

func (c *Partition[R, S, T]) Identifier() string {
	return c.table.Identifier()
}

func (c *Partition[R, S, T]) GetSchema() (*schema.RowSchema, error) {
	// get the schema for the table row type
	rowStruct := utils.InstanceOf[R]()
	return schema.SchemaFromStruct(rowStruct)
}

func (c *Partition[R, S, T]) initialiseConfig(tableConfigData config_data.ConfigData) error {
	if len(tableConfigData.GetHcl()) > 0 {
		cfg, err := parse.ParseConfig[S](tableConfigData)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}
		c.Config = cfg

		slog.Info("Table RowSourceImpl: config parsed", "config", c)

		// validate config
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}
	}
	return nil
}

// Collect executes the collection process. Tell our source to start collection
func (c *Partition[R, S, T]) Collect(ctx context.Context) (json.RawMessage, error) {

	// create empty status event#
	c.status = events.NewStatusEvent(c.req.ExecutionId)

	// tell our source to collect
	// this is a blocking call, but we will receive and processrow events during the execution
	err := c.source.Collect(ctx)
	if err != nil {
		return nil, err
	}

	slog.Info("Source collection complete - waiting for enrichment")

	// wait for all rows to be processed
	c.rowWg.Wait()

	// set the end time
	c.enrichTiming.End = time.Now()

	defer slog.Info("Enrichment complete")

	// notify observers of final status
	if err := c.NotifyObservers(ctx, c.status); err != nil {
		slog.Error("Table RowSourceImpl: error notifying observers of status", "error", err)
	}

	// now ask the source for its updated collection state data
	return c.source.GetCollectionStateJSON()
}

// Notify implements observable.Observer
// it handles all events which partitionFuncMap may receive (these will all come from the source)
func (c *Partition[R, S, T]) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	c.updateStatus(ctx, event)

	switch e := event.(type) {
	case *events.Row:
		return c.handleRowEvent(ctx, e)
	case *events.Error:
		slog.Error("Partition: error event received", "error", e.Err)
		return c.NotifyObservers(context.Background(), e)
	default:
		// ignore
		return nil
	}
}

func (c *Partition[R, S, T]) GetTiming() types.TimingCollection {
	return append(c.source.GetTiming(), c.enrichTiming)
}

func (c *Partition[R, S, T]) initSource(ctx context.Context, configData *config_data.SourceConfigData, connectionData *config_data.ConnectionConfigData) error {
	requestedSource := configData.Type

	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata, err := c.getSourceMetadata(requestedSource)
	if err != nil {
		return err
	}
	// ask factory to create and initialise the source for us
	// NOTE: we pass the original
	source, err := row_source.Factory.GetRowSource(ctx, configData, connectionData, sourceMetadata.Options...)
	if err != nil {
		return err
	}

	c.source = source

	// set mapper if source metadata specifies one
	if mapperFunc := sourceMetadata.MapperFunc; mapperFunc != nil {
		c.mapper = mapperFunc()
	}

	// add ourselves as an observer to our Source
	return c.source.AddObserver(c)
}

// ask table for it;s supported sources and put into map for ease of lookup
func (c *Partition[R, S, T]) getSupportedSources() map[string]*SourceMetadata[R] {
	supportedSources := c.table.SupportedSources(c.Config)
	// convert to a map for easy lookup
	sourceMap := make(map[string]*SourceMetadata[R])
	for _, s := range supportedSources {
		sourceMap[s.SourceName] = s
	}
	return sourceMap
}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (c *Partition[R, S, T]) updateStatus(ctx context.Context, e events.Event) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	c.status.Update(e)

	// send a status event periodically
	if time.Since(c.lastStatusEventTime) > statusUpdateInterval {
		// notify observers
		if err := c.NotifyObservers(ctx, c.status); err != nil {
			slog.Error("Table RowSourceImpl: error notifying observers of status", "error", err)
		}
		// update lastStatusEventTime
		c.lastStatusEventTime = time.Now()
	}
}

// handleRowEvent is invoked when a Row event is received - map, enrich and publish the row
func (c *Partition[R, S, T]) handleRowEvent(ctx context.Context, e *events.Row) error {
	c.rowWg.Add(1)
	defer c.rowWg.Done()

	// when all rows, a null row will be sent - DO NOT try to enrich this!
	row := e.Row
	if row == nil {
		// notify of nil row
		return c.NotifyObservers(ctx, events.NewRowEvent(e.ExecutionId, row, e.CollectionState))
	}

	// put data into an array as that is what mappers expect
	rows, err := c.mapRow(ctx, row)
	if err != nil {
		return fmt.Errorf("error mapping artifact: %w", err)
	}

	// set the enrich time if not already set
	c.enrichTiming.TryStart(constants.TimingEnrich)

	enrichStart := time.Now()

	// add partition to the enrichment fields
	enrichmentFields := e.EnrichmentFields
	enrichmentFields.TpPartition = c.req.PartitionData.Partition

	for _, mappedRow := range rows {
		// enrich the row
		enrichedRow, err := c.table.EnrichRow(mappedRow, enrichmentFields)
		if err != nil {
			return err
		}
		// validate the row
		if err := enrichedRow.Validate(); err != nil {
			// TODO #errors we need to include the raw row information in the error
			return err
		}

		// notify observers of enriched row
		if err := c.NotifyObservers(ctx, events.NewRowEvent(e.ExecutionId, enrichedRow, e.CollectionState)); err != nil {
			return err
		}

	}
	// update the enrich active duration
	c.enrichTiming.UpdateActiveDuration(time.Since(enrichStart))

	return nil
}

// mapRow applies any configured mappers to the raw rows
func (c *Partition[R, S, T]) mapRow(ctx context.Context, rawRow any) ([]R, error) {
	// if there is no mappers, just return the data as is
	if c.mapper == nil {
		row, ok := rawRow.(R)
		if !ok {
			return nil, fmt.Errorf("no mapper defined so expected source output to be %T, got %T", row, rawRow)
		}
		return []R{row}, nil
	}

	// mappers may return multiple rows so wrap data in a list
	var dataList = []any{rawRow}

	var errList []error

	// invoke each mapper in turn
	var mappedDataList []R
	for _, d := range dataList {
		mappedData, err := c.mapper.Map(ctx, d)
		if err != nil {
			// TODO #error should we give up immediately
			return nil, fmt.Errorf("error mapping artifact row: %w", err)
		} else {
			mappedDataList = append(mappedDataList, mappedData...)
		}
	}

	if len(errList) > 0 {
		return nil, fmt.Errorf("error mapping artifact rows: %w", errors.Join(errList...))
	}

	return mappedDataList, nil
}

func (c *Partition[R, S, T]) getSourceMetadata(requestedSource string) (sourceMetadata *SourceMetadata[R], err error) {
	// TODO: K #refactor make this function look/feel better
	defer func() {
		if sourceMetadata != nil {
			if len(c.req.CollectionState) > 0 {
				sourceMetadata.Options = append(sourceMetadata.Options, row_source.WithCollectionStateJSON(c.req.CollectionState))
			}
		}
	}()
	// get the supported sources for the table
	supportedSourceMap := c.getSupportedSources()

	// copy the requested source as we may change it
	sourceType := requestedSource

	// validate the source type is supported by this table
	sourceMetadata, ok := supportedSourceMap[sourceType]
	if ok {
		return sourceMetadata, nil
	}

	// so the requestedSource is not present in the map
	// the plugin may specify `artifact_source` as a supported source, meaning any artifact source is supported
	// this would cause the check to fail, as the requestedSource would be the name of a specific artifact source
	// whereas the map will have an entry keyed by `artifact_source`

	// check if such an entry exists - if it does, check if the requested source is an artifact source
	sourceMetadata, ok = supportedSourceMap[constants.ArtifactSourceIdentifier]
	if ok {
		// so the table supports artifact sources

		// is this source type an artifact source?
		if row_source.Factory.IsArtifactSource(sourceType) {
			// so requested type is an artifact source and the table supports artifact sources
			return sourceMetadata, nil
		}
	}
	// so the table does not support this source type
	return nil, fmt.Errorf("source type %s not supported by table %s", requestedSource, c.table.Identifier())

}
