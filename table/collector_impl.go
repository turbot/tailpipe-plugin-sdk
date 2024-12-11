package table

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"log/slog"
	"sync"
	"time"

	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// how often to send status events

const statusUpdateInterval = 250 * time.Millisecond

// JSONLChunkSizeis the number of  rows to write in each JSONL file
// - make the same size as duck db uses to infer schema (10000)
const JSONLChunkSize = 10000

// CollectorImpl is a generic implementation of the Collector interface
// it is responsible for coordinating the collection process and reporting status
// R is the type of the row struct
// S is the type of the partition config
// T is the type of the table
// U is the type of the connection
type CollectorImpl[R types.RowStruct, S parse.Config, T Table[R, S]] struct {
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

	// row buffer keyed by execution id
	// each row buffer is used to write a JSONL file
	rowBufferMap map[string][]any
	// mutex for row buffer map AND rowCountMap
	rowBufferLock sync.RWMutex
	// map of row counts keyed by execution id
	rowCountMap map[string]int
	// map of chunks written keyed by execution id
	chunkCountMap map[string]int

	writer ChunkWriter
}

func (c *CollectorImpl[R, S, T]) Init(ctx context.Context, req *types.CollectRequest) error {
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

	// if the plugin overrides this function it must call the base implementation
	c.rowBufferMap = make(map[string][]any)
	c.rowCountMap = make(map[string]int)
	c.chunkCountMap = make(map[string]int)
	// create writer
	c.writer = NewJSONLWriter(req.OutputPath)

	return nil
}

func (c *CollectorImpl[R, S, T]) Identifier() string {
	return c.table.Identifier()
}

// GetSchema returns the schema of the table if available
// for dynamic tables, the schema is only available at this if the config contains a schema
func (c *CollectorImpl[R, S, T]) GetSchema() (*schema.RowSchema, error) {
	rowStruct := utils.InstanceOf[R]()

	// if the table has a dynamic row, we can only return the schema is the config supports it
	if _, ok := any(rowStruct).(*DynamicRow); ok {

		// get the schema from the common fields
		s, err := schema.SchemaFromStruct(enrichment.CommonFields{})
		if err != nil {
			return nil, err
		}
		// set mode to partial
		s.Mode = schema.ModePartial

		// does the config implement GetSchema()
		// NOTE: the config may be nil here as this is called both from collection and from the factory
		// (if a Describe call has been made)
		if !helpers.IsNil(c.Config) {
			if d, ok := any(c.Config).(parse.DynamicTableConfig); ok {
				// return s from config, if defined (NO
				configuredSchema := d.GetSchema()
				if configuredSchema != nil {
					// if we have a schema from the config, use it (but do not overwrite the schema from the common fields)
					s.DefaultTo(configuredSchema)
				}
			}
		}

		return s, nil
	}

	// otherwise, return the schema from the row struct
	return schema.SchemaFromStruct(rowStruct)
}

func (c *CollectorImpl[R, S, T]) initialiseConfig(tableConfigData config_data.ConfigData) error {
	// default to empty config
	cfg := utils.InstanceOf[S]()
	if len(tableConfigData.GetHcl()) > 0 {
		var err error
		cfg, err = parse.ParseConfig[S](tableConfigData)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}

		slog.Info("Table RowSourceImpl: config parsed", "config", c)
	}
	c.Config = cfg

	// validate config
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid partition config: %w", err)
	}

	return nil
}

// Collect executes the collection process. Tell our source to start collection
func (c *CollectorImpl[R, S, T]) Collect(ctx context.Context) (int, int, error) {
	// create empty status event#
	c.status = events.NewStatusEvent(c.req.ExecutionId)

	// tell our source to collect
	// this is a blocking call, but we will receive and process row events during the execution
	err := c.source.Collect(ctx)
	if err != nil {
		return 0, 0, err
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

	return c.WriteRemainingRows(ctx, c.req.ExecutionId)
}

// Notify implements observable.Observer
// it handles all events which collectorFuncMap may receive (these will all come from the source)
func (c *CollectorImpl[R, S, T]) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	c.updateStatus(ctx, event)

	switch e := event.(type) {
	case *events.ArtifactDownloaded:
		// handle artifact downloaded event - we only act on this if the table implements ArtifactToJsonConverter
		return c.handleArtifactDownloaded(ctx, e)
	case *events.Row:
		// handle row event - map, enrich and publish the row
		// NOTE: we will not receive these if the table implements ArtifactToJsonConverter and therefore has a null loader
		return c.handleRowEvent(ctx, e)
	case *events.Error:
		slog.Error("CollectorImpl: error event received", "error", e.Err)
		return c.NotifyObservers(context.Background(), e)
	default:
		// ignore
		return nil
	}
}

func (c *CollectorImpl[R, S, T]) GetTiming() types.TimingCollection {
	return append(c.source.GetTiming(), c.enrichTiming)
}

func (c *CollectorImpl[R, S, T]) initSource(ctx context.Context, configData *config_data.SourceConfigData, connectionData *config_data.ConnectionConfigData) error {
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
	if mapper := sourceMetadata.Mapper; mapper != nil {
		c.mapper = mapper
	}

	// add ourselves as an observer to our Source
	return c.source.AddObserver(c)
}

func (c *CollectorImpl[R, S, T]) getSourceMetadata(requestedSource string) (sourceMetadata *SourceMetadata[R], err error) {
	// get the supported sources for the table
	supportedSourceMap := c.getSourceMetadataMap()

	// validate the requested source type is supported by this table
	sourceMetadata, ok := supportedSourceMap[requestedSource]
	if !ok {
		// the table may specify `artifact` as a supported source, meaning any artifact source is supported
		// this would cause the above check to fail, as the requestedSource would be the name of a specific artifact source
		// whereas the map will have an entry keyed by `artifact`

		// is the requested source an artifact source?
		if row_source.Factory.IsArtifactSource(requestedSource) {
			// check whether the supported sources map has an entry for 'artifact'
			sourceMetadata, ok = supportedSourceMap[constants.ArtifactSourceIdentifier]
		}

		// if we still don't have a source metadata, return an error
		if !ok {
			return nil, fmt.Errorf("source type %s not supported by table %s", requestedSource, c.table.Identifier())
		}
	}

	// so this source is supported

	// If the request includes collection state, add it to the source options
	if len(c.req.CollectionState) > 0 {
		sourceMetadata.Options = append(sourceMetadata.Options, row_source.WithCollectionStateJSON(c.req.CollectionState))
	}

	return sourceMetadata, nil
}

// ask table for it;s supported sources and put into map for ease of lookup
func (c *CollectorImpl[R, S, T]) getSourceMetadataMap() map[string]*SourceMetadata[R] {
	supportedSources := c.table.GetSourceMetadata(c.Config)
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
func (c *CollectorImpl[R, S, T]) updateStatus(ctx context.Context, e events.Event) {
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

func (c *CollectorImpl[R, S, T]) handleArtifactDownloaded(ctx context.Context, e *events.ArtifactDownloaded) error {
	if q, ok := any(c.table).(ArtifactToJsonConverter[S]); ok {
		executionId, err := context_values.ExecutionIdFromContext(ctx)
		if err != nil {
			return err
		}

		// get chunk count
		c.rowBufferLock.Lock()
		chunkNumber := c.chunkCountMap[e.ExecutionId]
		c.rowBufferLock.Unlock()

		// build JSON filename
		destFile := ExecutionIdToFileName(executionId, chunkNumber)

		// convert the artifact to JSONL
		query := q.GetArtifactConversionQuery(e.Info.Name, destFile, c.Config)

		// Open DuckDB
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return fmt.Errorf("failed to open DuckDB connection: %w", err)
		}
		defer db.Close() // Ensure the database connection is closed when done

		// Execute the query and retrieve the row count
		var rowCount int
		err = db.QueryRowContext(ctx, query).Scan(&rowCount)
		if err != nil {
			return fmt.Errorf("artifact conversion query failed: %w", err)
		}

		// Use the row count as needed
		fmt.Printf("Rows converted: %d\n", rowCount)

		// update rows and chunks written
		c.rowBufferLock.Lock()
		c.rowCountMap[e.ExecutionId] += rowCount
		c.chunkCountMap[e.ExecutionId]++
		c.rowBufferLock.Unlock()

	}
	return nil

}

// handleRowEvent is invoked when a Row event is received - map, enrich and publish the row
func (c *CollectorImpl[R, S, T]) handleRowEvent(ctx context.Context, e *events.Row) error {
	c.rowWg.Add(1)
	defer c.rowWg.Done()

	// when all rows, a null row will be sent - DO NOT try to enrich this!
	if e.Row == nil {
		// notify of nil row
		return c.NotifyObservers(ctx, events.NewRowEvent(e.ExecutionId, nil, e.CollectionState))
	}

	// put data into an array as that is what mappers expect
	mappedRow, err := c.mapRow(ctx, e.Row)
	if err != nil {
		return fmt.Errorf("error mapping artifact: %w", err)
	}

	// set the enrich time if not already set
	c.enrichTiming.TryStart(constants.TimingEnrich)
	enrichStart := time.Now()

	// add table and partition to the enrichment fields

	sourceMetadata := e.SourceMetadata
	sourceMetadata.CommonFields.TpTable = c.req.PartitionData.Table
	sourceMetadata.CommonFields.TpPartition = c.req.PartitionData.Partition

	// enrich the row
	enrichedRow, err := c.table.EnrichRow(mappedRow, c.Config, sourceMetadata)
	if err != nil {
		return err
	}
	// validate that the enriched row has required fields
	if err := enrichedRow.Validate(); err != nil {
		// TODO #errors we need to include the raw row information in the error
		return err
	}

	// update the enrich active duration
	c.enrichTiming.UpdateActiveDuration(time.Since(enrichStart))

	// buffer the enriched row and write to JSON file if buffer is full
	return c.onRowEnriched(ctx, enrichedRow, e.CollectionState)
}

// mapRow applies any configured mappers to the raw rows
func (c *CollectorImpl[R, S, T]) mapRow(ctx context.Context, rawRow any) (R, error) {
	var empty R
	// if there is no mapperFunc, just return the data as is
	if c.mapper == nil {
		// if no mapperFunc is defined, we expect the rawRow to be of type R - if not this is an error
		row, ok := rawRow.(R)
		if !ok {
			// TODO #error this is not raised in UI
			return empty, fmt.Errorf("no mapperFunc defined so expected source output to be %T, got %T", row, rawRow)
		}
		return row, nil
	}

	return c.mapper.Map(ctx, rawRow)
}

// onRowEnriched is called when a row has been enriched - it buffers the row and writes to JSONL file if buffer is full
func (c *CollectorImpl[R, S, T]) onRowEnriched(ctx context.Context, row R, collectionState json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	if c.rowBufferMap == nil {
		// this must mean the plugin has overridden the Init function and not called the base
		// this should be prevented by the validation test
		return errors.New("RowSourceImpl.Init must be called from the plugin Init function")
	}

	// add row to row buffer
	c.rowBufferLock.Lock()

	rowCount := c.rowCountMap[executionId]
	c.rowBufferMap[executionId] = append(c.rowBufferMap[executionId], row)
	rowCount++
	c.rowCountMap[executionId] = rowCount

	var rowsToWrite []any
	if len(c.rowBufferMap[executionId]) == JSONLChunkSize {
		rowsToWrite = c.rowBufferMap[executionId]
		c.rowBufferMap[executionId] = nil
	}
	c.rowBufferLock.Unlock()

	if numRowsToWrite := len(rowsToWrite); numRowsToWrite > 0 {
		return c.writeChunk(ctx, rowCount, rowsToWrite, collectionState)
	}

	return nil
}

// writeChunk writes a chunk of rows to a JSONL file
func (c *CollectorImpl[R, S, T]) writeChunk(ctx context.Context, rowCount int, rowsToWrite []any, collectionState json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// determine chunk number from rowCountMap
	chunkNumber := rowCount / JSONLChunkSize

	// check for final partial chunk
	if rowCount%JSONLChunkSize > 0 {
		chunkNumber++
	}
	slog.Debug("writing chunk to JSONL file", "chunk", chunkNumber, "rows", len(rowsToWrite))

	// convert row to a JSONL file
	err = c.writer.WriteChunk(ctx, rowsToWrite, chunkNumber)
	if err != nil {
		slog.Error("failed to write JSONL file", "error", err)
		return fmt.Errorf("failed to write JSONL file: %w", err)
	}

	// increment the chunk count
	c.rowBufferLock.Lock()
	c.chunkCountMap[executionId]++
	c.rowBufferLock.Unlock()

	// notify observers, passing the collection state data
	return c.OnChunk(ctx, chunkNumber, collectionState)
}

// OnChunk is called by the we have written a chunk of enriched rows to a [JSONL/CSV] file
// notify observers of the chunk
func (c *CollectorImpl[R, S, T]) OnChunk(ctx context.Context, chunkNumber int, collectionState json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber, collectionState)

	return c.NotifyObservers(ctx, e)
}

func (c *CollectorImpl[R, S, T]) WriteRemainingRows(ctx context.Context, executionId string) (int, int, error) {
	collectionState, err := c.source.GetCollectionStateJSON()
	if err != nil {
		return 0, 0, fmt.Errorf("error getting collection state: %w", err)
	}

	// get row count and the rows in the buffers
	c.rowBufferLock.Lock()
	rowCount := c.rowCountMap[executionId]
	rowsToWrite := c.rowBufferMap[executionId]
	chunksWritten := c.chunkCountMap[executionId]
	delete(c.rowBufferMap, executionId)
	delete(c.rowCountMap, executionId)

	c.rowBufferLock.Unlock()

	// tell our write to write any remaining rows
	if len(rowsToWrite) > 0 {
		if err := c.writeChunk(ctx, rowCount, rowsToWrite, collectionState); err != nil {
			slog.Error("failed to write final chunk", "error", err)
			return 0, 0, fmt.Errorf("failed to write final chunk: %w", err)
		}
		chunksWritten++
	}

	return rowCount, chunksWritten, nil
}
