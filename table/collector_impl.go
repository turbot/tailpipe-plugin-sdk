package table

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gtypes "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/error_types"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/filepaths"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// how often to send status events

const statusUpdateInterval = 250 * time.Millisecond

// JSONLChunkSize the number of  rows to write in each JSONL file
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
	mapper mappers.Mapper[R]

	// wait group to wait for all rows to be processed
	// this is incremented each time we receive a row event and decremented when we have processed it
	rowWg               sync.WaitGroup
	status              *events.Status
	lastStatusEventTime time.Time

	req *types.CollectRequest
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

func NewCollectorImpl[R types.RowStruct](table Table[R]) *CollectorImpl[R] {
	return &CollectorImpl[R]{Table: table}
}

func (c *CollectorImpl[R]) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req

	if err := c.initSource(ctx, req); err != nil {
		return err
	}

	// if the plugin overrides this function it must call the base implementation
	c.rowBufferMap = make(map[string][]any)
	c.rowCountMap = make(map[string]int)
	c.chunkCountMap = make(map[string]int)
	// get JSONL path
	jsonPath, err := filepaths.EnsureJSONLPath(req.CollectionTempDir)
	if err != nil {
		return fmt.Errorf("error getting JSONL path: %w", err)
	}
	// create writer
	c.writer = NewJSONLWriter(jsonPath)

	slog.Info("Initialise collector", "table", c.Table.Identifier(), "partition", req.PartitionName, "jsonPath", jsonPath)

	return nil
}

func (c *CollectorImpl[R]) Identifier() string {
	return c.Table.Identifier()
}

// GetSchema returns the schema of the table
func (c *CollectorImpl[R]) GetSchema() (*schema.TableSchema, error) {
	// if the table is a custom table, ask it for its schema
	if ct, ok := any(c.Table).(CustomTable); ok {
		// NOTE: for custom tables, the SourceColumn field is used for mapping _within_ the plugin,
		// not by the CLI for JSONL conversion
		s, err := ct.GetSchema()
		if err != nil {
			return nil, fmt.Errorf("error getting schema from custom table: %w", err)
		}
		// so for this schema, which will be used by the CLI, set SourceName to be the same as the ColumnName
		return s.WithSourceFieldsCleared(), nil
	}

	// otherwise, return the schema from the row struct
	rowStruct := utils.InstanceOf[R]()
	s, err := schema.SchemaFromStruct(rowStruct)
	if err != nil {
		return nil, fmt.Errorf("error getting schema from struct: %w", err)
	}

	// if the table implements DescriptionProvider, use this to populate the table description
	if getDesc, ok := c.Table.(schema.DescriptionProvider); ok {
		s.Description = getDesc.GetDescription()
	}

	return s, nil
}

// GetFromTime returns the 'resolved' from time of the source
func (c *CollectorImpl[S]) GetFromTime() *row_source.ResolvedFromTime {
	return c.source.GetFromTime()
}

// Collect executes the collection process. Tell our source to start collection
func (c *CollectorImpl[R]) Collect(ctx context.Context) (int, int, error) {
	slog.Info("Start collection", "table", c.Table.Identifier(), "partition", c.req.PartitionName)

	// create empty status event
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

	defer slog.Info("Enrichment complete")

	// notify observers of final status
	if err := c.NotifyObservers(ctx, c.status); err != nil {
		slog.Error("CollectorImpl: error notifying observers of status", "error", err)
	}

	return c.WriteRemainingRows(ctx, c.req.ExecutionId)
}

// Notify implements observable.Observer
// it receives events from the source
// it handles ONLY Row and Error events
func (c *CollectorImpl[R]) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	c.updateStatus(ctx, event)

	switch e := event.(type) {

	case *events.RowExtracted:
		// handle row event - map, enrich and publish the row
		return c.handleRowExtractedEvent(ctx, e)
	case *events.Error:
		// TODO determine whether this is a non-fatal error (in which case send an error event??) or a fatal error https://github.com/turbot/tailpipe-plugin-sdk/issues/72
		// in which case we need to terminate execution

		slog.Error("CollectorImpl: error event received", "error", e.Err)
		return c.NotifyObservers(context.Background(), e)
	default:
		// ignore
		// TODO pass other events through to observers
		// https://github.com/turbot/tailpipe-plugin-sdk/issues/24
		// https://github.com/turbot/tailpipe-plugin-sdk/issues/10
		return nil
	}
}

func (c *CollectorImpl[R]) initSource(ctx context.Context, req *types.CollectRequest) error {
	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata, err := c.getSourceMetadata(req.SourceData)
	if err != nil {
		return err
	}

	params := &row_source.RowSourceParams{
		SourceConfigData:    req.SourceData,
		ConnectionData:      req.ConnectionData,
		CollectionStatePath: req.CollectionStatePath,
		From:                req.From,
		CollectionTempDir:   req.CollectionTempDir,
	}

	// ask factory to create and initialise the source for us
	// NOTE: we pass the original
	source, err := row_source.Factory.GetRowSource(ctx, params, sourceMetadata.Options...)
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

func (c *CollectorImpl[R]) getSourceMetadata(sourceConfig *types.SourceConfigData) (sourceMetadata *SourceMetadata[R], err error) {
	// get the supported sources for the table
	supportedSourceMap, err := c.getSourceMetadataMap()
	if err != nil {
		return nil, err
	}
	requestedSource := sourceConfig.InstanceType
	// validate the requested source type is supported by this table
	sourceMetadata, ok := supportedSourceMap[requestedSource]
	if !ok {
		// the table may specify `artifact` as a supported source, meaning any artifact source is supported
		// this would cause the above check to fail, as the requestedSource would be the name of a specific artifact source
		// whereas the map will have an entry keyed by `artifact`

		// is the requested source an artifact source?
		// TODO #core how can we tell if any  given source is an artifact source?
		// // we need to ask it - either via the local source or if it is tremote we can connect to it and ask
		if row_source.IsArtifactSource(requestedSource) {
			// check whether the supported sources map has an entry for 'artifact'
			sourceMetadata, ok = supportedSourceMap[constants.ArtifactSourceIdentifier]
		}

		// if we still don't have a source metadata, return an error
		if !ok {
			return nil, fmt.Errorf("source type %s not supported by table %s", requestedSource, c.Table.Identifier())
		}
	}

	return sourceMetadata, nil
}

// ask table for it;s supported sources and put into map for ease of lookup
func (c *CollectorImpl[R]) getSourceMetadataMap() (map[string]*SourceMetadata[R], error) {
	supportedSources, err := c.Table.GetSourceMetadata()
	if err != nil {
		return nil, err
	}
	// convert to a map for easy lookup
	sourceMap := make(map[string]*SourceMetadata[R])
	for _, s := range supportedSources {
		sourceMap[s.SourceName] = s
	}
	return sourceMap, nil
}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (c *CollectorImpl[R]) updateStatus(ctx context.Context, e events.Event) {
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

// handleRowExtractedEvent is invoked when a RowExtracted event is received - map, enrich and publish the row
func (c *CollectorImpl[R]) handleRowExtractedEvent(ctx context.Context, e *events.RowExtracted) error {
	c.rowWg.Add(1)
	defer c.rowWg.Done()

	sourceEnrichment := e.SourceEnrichment
	sourceLocation := gtypes.SafeString(sourceEnrichment.CommonFields.TpSourceLocation)
	if sourceLocation == "" {
		sourceLocation = gtypes.SafeString(sourceEnrichment.CommonFields.TpSourceName)
	}

	// put data into an array as that is what mappers expect
	mappedRow, err := c.mapRow(ctx, e.Row)
	if err != nil {
		// call onRowError to update status with the row error, we do not return error to source
		return c.onRowError(ctx, sourceLocation, error_types.RowOperationTypeMapping, err)
	}

	// add table and partition to the enrichment fields
	sourceEnrichment.CommonFields.TpTable = c.req.TableName
	sourceEnrichment.CommonFields.TpPartition = c.req.PartitionName

	// enrich the row
	enrichedRow, err := c.Table.EnrichRow(mappedRow, sourceEnrichment)
	if err != nil {
		// call onRowError to update status with the row error, we do not return error to source
		return c.onRowError(ctx, sourceLocation, error_types.RowOperationTypeEnrichment, err)
	}
	// validate that the enriched row has required fields
	if err = enrichedRow.Validate(); err != nil {
		// call onRowError to update status with the row error, we do not return error to source
		return c.onRowError(ctx, sourceLocation, error_types.RowOperationTypeValidation, err)
	}

	// buffer the enriched row and write to JSON file if buffer is full
	return c.onRowEnriched(ctx, enrichedRow)
}

// mapRow applies any configured mappers to the raw rows
func (c *CollectorImpl[R]) mapRow(ctx context.Context, rawRow any) (R, error) {
	var empty R
	// if there is no mapper, just return the data as is
	if c.mapper == nil {
		// if no mapper is defined, we expect the rawRow to be of type R - if not this is an error
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
func (c *CollectorImpl[R]) onRowEnriched(ctx context.Context, row R) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// update status
	c.status.OnRowEnriched()

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
		return c.writeChunk(ctx, rowCount, rowsToWrite)
	}

	return nil
}

// onRowError is called when a row operation (map/enrich/validate) fails, it updates our status but doesn't return an error back to source
func (c *CollectorImpl[R]) onRowError(_ context.Context, source string, operation error_types.RowOperationType, err error) error {
	// if called without an error do nothing
	if err == nil {
		return nil
	}

	// ensure is a RowError (or convert to one)
	out := error_types.EnsureRowError(source, operation, err)

	// update status
	c.status.OnRowError(out)

	// don't return error back to source, we update this in status
	return nil
}

// writeChunk writes a chunk of rows to a JSONL file
func (c *CollectorImpl[R]) writeChunk(ctx context.Context, rowCount int, rowsToWrite []any) error {
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
	return c.OnChunk(ctx, chunkNumber)
}

// OnChunk is called by the we have written a chunk of enriched rows to a [JSONL/CSV] file
// notify observers of the chunk
func (c *CollectorImpl[R]) OnChunk(ctx context.Context, chunkNumber int) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber)

	if err = c.NotifyObservers(ctx, e); err != nil {
		return fmt.Errorf("error notifying observers of chunk: %w", err)
	}

	// TODO collection state should we save here???
	// tell source to save collection state
	if err := c.source.SaveCollectionState(); err != nil {
		return fmt.Errorf("error saving collection state: %w", err)
	}
	return nil
}

func (c *CollectorImpl[R]) WriteRemainingRows(ctx context.Context, executionId string) (int, int, error) {
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
		if err := c.writeChunk(ctx, rowCount, rowsToWrite); err != nil {
			slog.Error("failed to write final chunk", "error", err)
			return 0, 0, fmt.Errorf("failed to write final chunk: %w", err)
		}
		chunksWritten++
	}

	return rowCount, chunksWritten, nil
}
