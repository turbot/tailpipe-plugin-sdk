package table

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
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
type CollectorImpl[R types.RowStruct] struct {
	observable.ObservableImpl

	Table  Table[R]
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

func (c *CollectorImpl[R]) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req

	slog.Info("CollectorImpl: Collect", "Table", c.Table.Identifier())
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

func (c *CollectorImpl[R]) Identifier() string {
	return c.Table.Identifier()
}

// GetSchema returns the schema of the table
func (c *CollectorImpl[R]) GetSchema() (*schema.RowSchema, error) {
	rowStruct := utils.InstanceOf[R]()

	// if the table has a dynamic row, we can only return the schema is the config supports it
	if d, ok := any(rowStruct).(*DynamicRow); ok {
		// we must have a custom table
		customTable := c.req.CustomTable
		if customTable == nil {
			return nil, fmt.Errorf("table %s has dynamic row but no custom table definition", c.Table.Identifier())
		}
		return d.ResolveSchema(customTable)
	}

	// otherwise, return the schema from the row struct
	return schema.SchemaFromStruct(rowStruct)
}

// Collect executes the collection process. Tell our source to start collection
func (c *CollectorImpl[R]) Collect(ctx context.Context) (int, int, error) {
	// create empty status event#
	c.status = events.NewStatusEvent(c.req.ExecutionId)

	// tell our source to collect
	// this is a blocking call, but we will receive and process row events during the execution
	err := c.source.Collect(ctx)
	if err != nil {
		return 0, 0, err
	}

	return 0, 0, fmt.Errorf("not implemented")

	slog.Info("Source collection complete - waiting for enrichment")

	// wait for all rows to be processed
	c.rowWg.Wait()

	// set the end time
	c.enrichTiming.End = time.Now()

	defer slog.Info("Enrichment complete")

	// notify observers of final status
	if err := c.NotifyObservers(ctx, c.status); err != nil {
		slog.Error("CollectorImpl: error notifying observers of status", "error", err)
	}

	return c.WriteRemainingRows(ctx, c.req.ExecutionId)
}

// Notify implements observable.Observer
// it handles all events which collectorFuncMap may receive (these will all come from the source)
func (c *CollectorImpl[R]) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	c.updateStatus(ctx, event)

	switch e := event.(type) {

	case *events.Row:
		// handle row event - map, enrich and publish the row
		return c.handleRowEvent(ctx, e)
	case *events.Error:
		slog.Error("CollectorImpl: error event received", "error", e.Err)
		return c.NotifyObservers(context.Background(), e)
	default:
		// ignore
		return nil
	}
}

func (c *CollectorImpl[R]) GetTiming() types.TimingCollection {
	return append(c.source.GetTiming(), c.enrichTiming)
}

func (c *CollectorImpl[R]) initSource(ctx context.Context, configData *config_data.SourceConfigData, connectionData *config_data.ConnectionConfigData) error {
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

func (c *CollectorImpl[R]) getSourceMetadata(requestedSource string) (sourceMetadata *SourceMetadata[R], err error) {
	// get the supported sources for the table
	supportedSourceMap := c.getSourceMetadataMap()

	// validate the requested source type is supported by this table
	sourceMetadata, ok := supportedSourceMap[requestedSource]
	if !ok {
		// the table may specify `artifact` as a supported source, meaning any artifact source is supported
		// this would cause the above check to fail, as the requestedSource would be the name of a specific artifact source
		// whereas the map will have an entry keyed by `artifact`

		// is the requested source an artifact source?
		if row_source.IsArtifactSource(requestedSource) {
			// check whether the supported sources map has an entry for 'artifact'
			sourceMetadata, ok = supportedSourceMap[constants.ArtifactSourceIdentifier]
		}

		// if we still don't have a source metadata, return an error
		if !ok {
			return nil, fmt.Errorf("source type %s not supported by table %s", requestedSource, c.Table.Identifier())
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
func (c *CollectorImpl[R]) getSourceMetadataMap() map[string]*SourceMetadata[R] {
	supportedSources := c.Table.GetSourceMetadata()
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
func (c *CollectorImpl[R]) updateStatus(ctx context.Context, e events.Event) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	c.status.Update(e)

	// send a status event periodically
	if time.Since(c.lastStatusEventTime) > statusUpdateInterval {
		// notify observers
		if err := c.NotifyObservers(ctx, c.status); err != nil {
			slog.Error("tableName RowSourceImpl: error notifying observers of status", "error", err)
		}
		// update lastStatusEventTime
		c.lastStatusEventTime = time.Now()
	}
}

// handleRowEvent is invoked when a Row event is received - map, enrich and publish the row
func (c *CollectorImpl[R]) handleRowEvent(ctx context.Context, e *events.Row) error {
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
	sourceMetadata.CommonFields.TpTable = c.req.TableName
	sourceMetadata.CommonFields.TpPartition = c.req.PartitionName

	// enrich the row
	enrichedRow, err := c.Table.EnrichRow(mappedRow, sourceMetadata)
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
func (c *CollectorImpl[R]) mapRow(ctx context.Context, rawRow any) (R, error) {
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

	// if there is a custom table, pass the schema to the mapper
	var opts []MapOption[R]
	if c.req.CustomTable != nil {
		opts = append(opts, WithSchema[R](c.req.CustomTable.Schema))
	}

	return c.mapper.Map(ctx, rawRow, opts...)
}

// onRowEnriched is called when a row has been enriched - it buffers the row and writes to JSONL file if buffer is full
func (c *CollectorImpl[R]) onRowEnriched(ctx context.Context, row R, collectionState json.RawMessage) error {
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
func (c *CollectorImpl[R]) writeChunk(ctx context.Context, rowCount int, rowsToWrite []any, collectionState json.RawMessage) error {
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
func (c *CollectorImpl[R]) OnChunk(ctx context.Context, chunkNumber int, collectionState json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber, collectionState)

	return c.NotifyObservers(ctx, e)
}

func (c *CollectorImpl[R]) WriteRemainingRows(ctx context.Context, executionId string) (int, int, error) {
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
