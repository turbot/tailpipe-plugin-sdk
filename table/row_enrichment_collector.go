package table

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/error_types"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/filepaths"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// JSONLChunkSize the number of  rows to write in each JSONL file
// - make the same size as duck db uses to infer schema (10000)
const JSONLChunkSize = 10000

// RowEnrichmentCollector is a generic implementation of the Collector interface
// it is responsible for coordinating the collection process and reporting status
// R is the type of the row struct
type RowEnrichmentCollector[R types.RowStruct] struct {
	CollectorImpl[R]

	table  Table[R]
	mapper mappers.Mapper[R]
	// buffer to store the enriched rows before writing each JSONL
	rowBuffer []any
	// mutex for row buffer map
	rowBufferLock sync.RWMutex
	// how may rows have we written
	rowCount int64
	// how may chunks have we written - used only for status reporting
	chunkCount int32

	writer ChunkWriter

	// map of headers, keyed by the artifact path
	// used for delimited artifacts with a header row
	headers    map[string][]string
	headersMut sync.RWMutex
}

func NewRowEnrichmentCollector[R types.RowStruct](table Table[R]) *RowEnrichmentCollector[R] {
	return &RowEnrichmentCollector[R]{
		table:     table,
		rowBuffer: make([]any, 0, JSONLChunkSize),
		headers:   make(map[string][]string),
	}
}

func (c *RowEnrichmentCollector[R]) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req
	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata, err := c.getSourceMetadata(req.SourceData)
	if err != nil {
		return err
	}

	// set mapper if source metadata specifies one
	if mapper := sourceMetadata.Mapper; mapper != nil {
		c.mapper = mapper
	}

	// create the source
	if err := c.initSource(ctx, req, sourceMetadata); err != nil {
		return err
	}

	// add ourselves as an observer to our source
	if err := c.source.AddObserver(c); err != nil {
		return err
	}

	// create writer
	jsonPath, err := filepaths.EnsureJSONLPath(req.CollectionTempDir)
	if err != nil {
		return fmt.Errorf("error getting JSONL path: %w", err)
	}
	// set the path to write JSONL files to
	c.jsonPath = jsonPath
	// create the writer
	c.writer = NewJSONLWriter(jsonPath)

	slog.Info("Initialise collector", "table", c.table.Identifier(), "partition", req.PartitionName, "jsonPath", jsonPath)

	return nil
}

func (c *RowEnrichmentCollector[R]) Identifier() string {
	return c.table.Identifier()
}

// GetSchema returns the schema of the table
func (c *RowEnrichmentCollector[R]) GetSchema() (*schema.TableSchema, error) {
	// if the table is a custom table, ask it for its schema
	if ct, ok := any(c.table).(CustomTable); ok {
		s, err := ct.GetSchema()
		if err != nil {
			return nil, fmt.Errorf("error getting schema from custom table: %w", err)
		}
		// NOTE: for row enrichment custom tables, the SourceColumn field is used for mapping _within_ the plugin,
		// not by the CLI for JSONL conversion
		// DynamicRow.Enrich executes the source-output field name mapping by calling schema.MapRow
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

	// normalize the column types to lower case to ensure consistency
	s.NormaliseColumnTypes()

	return s, nil
}

// Collect executes the collection process. Tell our source to start collection
func (c *RowEnrichmentCollector[R]) Collect(ctx context.Context) (int64, int32, error) {
	// call base method to Collect rows from the source
	if _, _, err := c.CollectorImpl.Collect(ctx); err != nil {
		return 0, 0, err
	}

	// write any remaining rows in the buffer to the JSONL file
	return c.writeRemainingRows(ctx)
}

// Notify implements observable.Observer
// it receives events from the source
// it handles ONLY Row and Error events
func (c *RowEnrichmentCollector[R]) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	c.updateStatus(ctx, event)

	// NOTE: we do not pass error events to CLI - we have added to the status instead
	switch e := event.(type) {
	case *events.Header:
		// handle header event - store the header for this artifact
		c.handleHeaderEvent(e)
		return nil
	case *events.RowExtracted:
		// handle row event - map, enrich and publish the row
		return c.handleRowExtractedEvent(ctx, e)
	default:
		// ignore
		return nil
	}
}

// ask table for it;s supported sources and put into map for ease of lookup
func (c *RowEnrichmentCollector[R]) getSourceMetadata(sourceConfig *types.SourceConfigData) (*SourceMetadata[R], error) {
	supportedSources, err := c.table.GetSourceMetadata()
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
			return nil, fmt.Errorf("source type %s not supported by table %s", requestedSource, c.table.Identifier())
		}
	}

	return sourceMetadata, nil
}

// handleHeaderEvent is invoked when a Header event is received
// if we have a mapper and it has an OnHeader method, call it
func (c *RowEnrichmentCollector[R]) handleHeaderEvent(e *events.Header) {
	// each artifact may have a different header, so store a map of headers, keyed by the source location
	sourceLocation := e.Info.SourceEnrichment.ResolveSourceLocation()

	if sourceLocation == "" {
		// if source location is not set, we cannot store the header
		// (we use TpSourceLocation to read headers from the map so we need to use that as key)
		return
	}
	c.headersMut.Lock()
	defer c.headersMut.Unlock()

	c.headers[sourceLocation] = e.Header
}

// handleRowExtractedEvent is invoked when a RowExtracted event is received - map, enrich and publish the row
func (c *RowEnrichmentCollector[R]) handleRowExtractedEvent(ctx context.Context, e *events.RowExtracted) error {
	c.collectionWg.Add(1)
	defer c.collectionWg.Done()

	sourceEnrichment := e.SourceEnrichment
	sourceLocation := e.SourceEnrichment.ResolveSourceLocation()

	mappedRow, err := c.mapRow(ctx, e.Row, sourceLocation)
	if err != nil {
		// call onRowError to update status with the row error, we do not return error to source
		return c.onRowError(ctx, sourceLocation, error_types.RowOperationTypeMapping, err, e.Row)
	}

	// add table and partition to the enrichment fields
	sourceEnrichment.CommonFields.TpTable = c.req.TableName
	sourceEnrichment.CommonFields.TpPartition = c.req.PartitionName

	// enrich the row
	enrichedRow, err := c.table.EnrichRow(mappedRow, sourceEnrichment)
	if err != nil {
		// call onRowError to update status with the row error, we do not return error to source
		return c.onRowError(ctx, sourceLocation, error_types.RowOperationTypeEnrichment, err, mappedRow)
	}
	// validate that the enriched row has required fields
	if err = enrichedRow.Validate(); err != nil {
		// call onRowError to update status with the row error, we do not return error to source
		return c.onRowError(ctx, sourceLocation, error_types.RowOperationTypeValidation, err, enrichedRow)
	}

	// buffer the enriched row and write to JSON file if buffer is full
	return c.onRowEnriched(ctx, enrichedRow)
}

// mapRow applies any configured mappers to the raw rows
func (c *RowEnrichmentCollector[R]) mapRow(ctx context.Context, rawRow any, sourceLocation string) (R, error) {
	var opts []mappers.MapOption[R]

	// see if we have headers for this source location
	c.headersMut.RLock()
	header, ok := c.headers[sourceLocation]
	c.headersMut.RUnlock()
	if ok {
		opts = append(opts, mappers.WithHeader[R](header))
	}

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

	return c.mapper.Map(ctx, rawRow, opts...)
}

// onRowEnriched is called when a row has been enriched - it buffers the row and writes to JSONL file if buffer is full
func (c *RowEnrichmentCollector[R]) onRowEnriched(ctx context.Context, row R) error {
	// update status
	c.status.OnRowEnriched()

	// ensure all row buffer handling is within a lock
	c.rowBufferLock.Lock()

	// add row to row buffer
	c.rowBuffer = append(c.rowBuffer, row)
	// increment the count
	c.rowCount++

	// determine whether we need to write a jsonl file - is buffer full
	if len(c.rowBuffer) <= JSONLChunkSize {
		// not enough rows to write yet - just unlock the lock and return
		c.rowBufferLock.Unlock()
		return nil
	}

	// so we do need to write a JSONL file
	// put the rows to write in a temp variable and clear the buffer so other threads can keep writing
	rowsToWrite := c.rowBuffer
	// TODO make more efficient - preallocate pair of buffers
	c.rowBuffer = make([]any, 0, JSONLChunkSize)
	// get the current chunk number into a local var
	chunkNumber := c.chunkCount
	// increment the chunk count (inside the lock)
	c.chunkCount++

	// unlock the row buffer lock before writing - we can write concurrently as long we lock the buffer and rowCount correctly
	c.rowBufferLock.Unlock()

	// write the chunk to the JSONL file
	return c.writeChunk(ctx, rowsToWrite, chunkNumber)
}

// onRowError is called when a row operation (map/enrich/validate) fails, it updates our status but doesn't return an error back to source
func (c *CollectorImpl[R]) onRowError(_ context.Context, source string, operation error_types.RowOperationType, err error, row any) error {
	// if called without an error do nothing
	if err == nil {
		return nil
	}

	// ensure is a RowError (or convert to one)
	rowError := error_types.EnsureRowError(source, operation, err)

	// log the error
	slog.Error(fmt.Sprintf(fmt.Sprintf("failed %s row", operation), "error", rowError, "row", row))

	// update status
	c.status.OnRowError(rowError)

	// don't return error back to source, we update this in status
	return nil
}

// writeChunk writes a chunk of rows to a JSONL file
func (c *RowEnrichmentCollector[R]) writeChunk(ctx context.Context, rowsToWrite []any, chunkNumber int32) error {
	// wait for pause
	// we call this in addition to the events being blocked to avoid row events which were sent _before_ the pause causing
	// us to write JSONL files _after_ we are paused (allowing for enrichment time)
	c.BlockWhilePaused(ctx)

	slog.Debug("writing chunk to JSONL file", "chunk", chunkNumber, "rows", len(rowsToWrite))

	// convert row to a JSONL file
	err := c.writer.WriteChunk(ctx, rowsToWrite, chunkNumber)
	if err != nil {
		slog.Error("failed to write JSONL file", "error", err)
		return fmt.Errorf("failed to write JSONL file: %w", err)
	}

	// notify observers, passing the collection state data
	return c.onChunk(ctx, chunkNumber)
}

func (c *RowEnrichmentCollector[R]) writeRemainingRows(ctx context.Context) (int64, int32, error) {
	// NOTE: no need for atomic operation here as this will only be called once after everything is done

	// tell our writer to write any remaining rows
	if len(c.rowBuffer) > 0 {
		if err := c.writeChunk(ctx, c.rowBuffer, c.chunkCount); err != nil {
			slog.Error("failed to write final chunk", "error", err)
			return 0, 0, fmt.Errorf("failed to write final chunk: %w", err)
		}
	}

	return c.rowCount, c.chunkCount, nil
}
