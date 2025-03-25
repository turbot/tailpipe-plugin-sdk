package table

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/filepaths"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// JSONLChunkSize the number of  rows to write in each JSONL file
// - make the same size as duck db uses to infer schema (10000)
const JSONLChunkSize = 10000

// RowEnrichmentCollector is a generic implementation of the Collector interface
// it is responsible for coordinating the collection process and reporting status
// R is the type of the row struct
type RowEnrichmentCollector[R types.RowStruct] struct {
	observable.ObservableImpl

	req    *types.CollectRequest
	table  Table[R]
	source row_source.RowSource

	// wait group to wait for all rows to be processed
	// this is incremented each time we receive a row event and decremented when we have processed it
	rowWg  sync.WaitGroup
	status *events.Status

	lastStatusEventTime time.Time

	mapper mappers.Mapper[R]

	// row buffer keyed by execution id
	// each row buffer is used to write a JSONL file
	// mutex to protect the row buffer
	rowBufferLock sync.Mutex
	rowBuffer     []any
	rowCount      int64
	chunkCount    int64
	writer        ChunkWriter
}

func NewRowEnrichmentCollector[R types.RowStruct](table Table[R]) *RowEnrichmentCollector[R] {
	return &RowEnrichmentCollector[R]{
		table:     table,
		rowBuffer: make([]any, 0, JSONLChunkSize),
	}
}

func (c *RowEnrichmentCollector[R]) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req

	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata, err := getSourceMetadata(req.SourceData, c.table)
	if err != nil {
		return err
	}

	if err := c.initSource(ctx, req, sourceMetadata); err != nil {
		return err
	}

	// if the plugin overrides this function it must call the base implementation
	// get JSONL path
	jsonPath, err := filepaths.EnsureJSONLPath(req.CollectionTempDir)
	if err != nil {
		return fmt.Errorf("error getting JSONL path: %w", err)
	}
	// create writer
	c.writer = NewJSONLWriter(jsonPath)

	slog.Info("Initialise collector", "table", c.table.Identifier(), "partition", req.PartitionName, "jsonPath", jsonPath)

	return nil
}

func (c *RowEnrichmentCollector[R]) Identifier() string {
	return c.table.Identifier()
}

// GetFromTime returns the 'resolved' from time of the source
func (c *RowEnrichmentCollector[R]) GetFromTime() *row_source.ResolvedFromTime {
	return c.source.GetFromTime()
}

// GetSchema returns the schema of the table
func (c *RowEnrichmentCollector[R]) GetSchema() (*schema.TableSchema, error) {
	// if the table is a custom table, ask it for its schema
	if ct, ok := any(c.table).(CustomTable); ok {
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
	if getDesc, ok := c.table.(schema.DescriptionProvider); ok {
		s.Description = getDesc.GetDescription()
	}

	return s, nil
}

// Collect executes the collection process. Tell our source to start collection
func (c *RowEnrichmentCollector[R]) Collect(ctx context.Context) (int, int, error) {
	slog.Info("Start collection", "table", c.table.Identifier(), "partition", c.req.PartitionName)

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
		slog.Error("RowEnrichmentCollector: error notifying observers of status", "error", err)
	}

	return c.writeRemainingRows(ctx, c.req.ExecutionId)
}

// Notify implements observable.Observer
// it receives events from the source
// it handles ONLY Row and Error events
func (c *RowEnrichmentCollector[R]) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	c.updateStatus(ctx, event)

	switch e := event.(type) {

	case *events.RowExtracted:
		// handle row event - map, enrich and publish the row
		return c.handleRowExtractedEvent(ctx, e)
	case *events.Error:
		slog.Error("RowEnrichmentCollector: error event received", "error", e.Err)
		return c.NotifyObservers(context.Background(), e)
	default:
		// ignore
		return nil
	}
}

func (c *RowEnrichmentCollector[R]) initSource(ctx context.Context, req *types.CollectRequest, sourceMetadata *SourceMetadata[R]) error {
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

// handleRowExtractedEvent is invoked when a RowExtracted event is received - map, enrich and publish the row
func (c *RowEnrichmentCollector[R]) handleRowExtractedEvent(ctx context.Context, e *events.RowExtracted) error {
	c.rowWg.Add(1)
	defer c.rowWg.Done()

	// put data into an array as that is what mappers expect
	mappedRow, err := c.mapRow(ctx, e.Row)
	if err != nil {
		return fmt.Errorf("error mapping artifact: %w", err)
	}

	// add table and partition to the enrichment fields

	sourceEnrichment := e.SourceEnrichment
	sourceEnrichment.CommonFields.TpTable = c.req.TableName
	sourceEnrichment.CommonFields.TpPartition = c.req.PartitionName

	// enrich the row
	enrichedRow, err := c.table.EnrichRow(mappedRow, sourceEnrichment)
	if err != nil {
		return err
	}
	// validate that the enriched row has required fields
	if err := enrichedRow.Validate(); err != nil {
		// TODO #errors we need to include the raw row information in the error
		return err
	}

	// buffer the enriched row and write to JSON file if buffer is full
	return c.onRowEnriched(ctx, enrichedRow)
}

// mapRow applies any configured mappers to the raw rows
func (c *RowEnrichmentCollector[R]) mapRow(ctx context.Context, rawRow any) (R, error) {
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
func (c *RowEnrichmentCollector[R]) onRowEnriched(ctx context.Context, row R) error {
	// update status
	c.status.OnRowEnriched()

	atomic.AddInt64(&c.rowCount, 1)

	c.rowBufferLock.Lock()
	var rowsToWrite []any
	if len(c.rowBuffer) == JSONLChunkSize {
		rowsToWrite = c.rowBuffer
		c.rowBuffer = make([]any, 0, JSONLChunkSize)
	}
	c.rowBufferLock.Unlock()

	if numRowsToWrite := len(rowsToWrite); numRowsToWrite > 0 {
		return c.writeChunk(ctx, int(atomic.LoadInt64(&c.rowCount)), rowsToWrite)
	}

	return nil
}

// writeChunk writes a chunk of rows to a JSONL file
func (c *RowEnrichmentCollector[R]) writeChunk(ctx context.Context, rowCount int, rowsToWrite []any) error {
	// determine chunk number from rowCountMap
	chunkNumber := rowCount / JSONLChunkSize

	// check for final partial chunk
	if rowCount%JSONLChunkSize > 0 {
		chunkNumber++
	}
	slog.Debug("writing chunk to JSONL file", "chunk", chunkNumber, "rows", len(rowsToWrite))

	// convert row to a JSONL file
	err := c.writer.WriteChunk(ctx, rowsToWrite, chunkNumber)
	if err != nil {
		slog.Error("failed to write JSONL file", "error", err)
		return fmt.Errorf("failed to write JSONL file: %w", err)
	}

	// increment the chunk count
	atomic.AddInt64(&c.chunkCount, 1)

	// notify observers, passing the collection state data
	return c.onChunk(ctx, chunkNumber)
}

// onChunk is called by the we have written a chunk of enriched rows to a [JSONL/CSV] file
// notify observers of the chunk
func (c *RowEnrichmentCollector[R]) onChunk(ctx context.Context, chunkNumber int) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber)

	if err = c.NotifyObservers(ctx, e); err != nil {
		return fmt.Errorf("error notifying observers of chunk: %w", err)
	}

	// tell source to save collection state
	if err := c.source.SaveCollectionState(); err != nil {
		return fmt.Errorf("error saving collection state: %w", err)
	}
	return nil
}

func (c *RowEnrichmentCollector[R]) writeRemainingRows(ctx context.Context, executionId string) (int, int, error) {
	// NOTE: not need for atomic operation here as this will only be called once after everything is done

	// tell our writer to write any remaining rows
	if len(c.rowBuffer) > 0 {
		if err := c.writeChunk(ctx, int(c.rowCount), c.rowBuffer); err != nil {
			slog.Error("failed to write final chunk", "error", err)
			return 0, 0, fmt.Errorf("failed to write final chunk: %w", err)
		}
		c.chunkCount++
	}

	return int(c.rowCount), int(c.chunkCount), nil
}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (c *RowEnrichmentCollector[R]) updateStatus(ctx context.Context, e events.Event) {
	c.status.Update(e)

	// send a status event periodically
	if time.Since(c.lastStatusEventTime) > events.StatusUpdateInterval {
		// notify observers
		if err := c.NotifyObservers(ctx, c.status); err != nil {
			slog.Error("tableName RowSourceImpl: error notifying observers of status", "error", err)
		}
		// update lastStatusEventTime
		c.lastStatusEventTime = time.Now()
	}
}

// ask table for it;s supported sources and put into map for ease of lookup
func getSourceMetadata[R types.RowStruct](sourceConfig *types.SourceConfigData, table Table[R]) (*SourceMetadata[R], error) {
	supportedSources, err := table.GetSourceMetadata()
	if err != nil {
		return nil, err
	}
	// convert to a map for easy lookup
	supportedSourceMap := make(map[string]*SourceMetadata[R])
	for _, s := range supportedSources {
		supportedSourceMap[s.SourceName] = s
	}

	// get the supported sources for the table
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
			return nil, fmt.Errorf("source type %s not supported by table %s", requestedSource, table.Identifier())
		}
	}

	return sourceMetadata, nil
}
