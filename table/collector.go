package table

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// how often to send status events
const statusUpdateInterval = 250 * time.Millisecond

// RowCollector is responsible for coordinating the collection process and reporting status
type RowCollector[R types.RowStruct] struct {
	observable.ObservableImpl

	table  Table[R]
	source row_source.RowSource
	mapper Mapper[R]

	// wait group to wait for all rows to be processed
	// this is incremented each time we receive a row event and decremented when we have processed it
	rowWg               sync.WaitGroup
	status              *events.Status
	lastStatusEventTime time.Time

	statusLock   sync.RWMutex
	enrichTiming types.Timing
	req          *types.CollectRequest
}

// Collect executes the collection process. Tell our source to start collection
func (c *RowCollector[R]) Collect(ctx context.Context, req *types.CollectRequest) (json.RawMessage, error) {
	c.req = req

	slog.Info("Table RowSourceImpl: Collect", "table", c.table.Identifier())
	if err := c.initSource(ctx, req.SourceData); err != nil {
		return nil, err
	}
	slog.Info("Start collection")

	// create empty status event#
	c.status = events.NewStatusEvent(req.ExecutionId)

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

func (c *RowCollector[R]) initSource(ctx context.Context, configData *config_data.SourceConfigData) error {
	requestedSource := configData.Type

	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata, err := c.getSourceMetadata(requestedSource)
	if err != nil {
		return err
	}
	// ask factory to create and initialise the source for us
	// NOTE: we pass the original
	source, err := row_source.Factory.GetRowSource(ctx, configData, sourceMetadata.Options...)
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
func (c *RowCollector[R]) getSupportedSources() map[string]*SourceMetadata[R] {
	supportedSources := c.table.SupportedSources()
	// convert to a map for easy lookup
	sourceMap := make(map[string]*SourceMetadata[R])
	for _, s := range supportedSources {
		sourceMap[s.SourceName] = s
	}
	return sourceMap
}

// Notify implements observable.Observer
// it handles all events which tableFuncMap may receive (these will all come from the source)
func (c *RowCollector[R]) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	c.updateStatus(ctx, event)

	switch e := event.(type) {
	case *events.Row:
		return c.handleRowEvent(ctx, e)
	case *events.Error:
		return c.handeErrorEvent(e)
	default:
		// ignore
		return nil
	}
}

func (c *RowCollector[R]) GetTiming() types.TimingCollection {
	return append(c.source.GetTiming(), c.enrichTiming)
}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (c *RowCollector[R]) updateStatus(ctx context.Context, e events.Event) {
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
func (c *RowCollector[R]) handleRowEvent(ctx context.Context, e *events.Row) error {
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

func (c *RowCollector[R]) handeErrorEvent(e *events.Error) error {
	slog.Error("Table RowSourceImpl: error event received", "error", e.Err)
	c.NotifyObservers(context.Background(), e)
	return nil
}

// mapRow applies any configured mappers to the raw rows
func (c *RowCollector[R]) mapRow(ctx context.Context, rawRow any) ([]R, error) {
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

func (c *RowCollector[R]) getSourceMetadata(requestedSource string) (*SourceMetadata[R], error) {
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
