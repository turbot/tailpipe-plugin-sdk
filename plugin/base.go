package plugin

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

/* TODO VALIDATION
- check plugin ref is stored in collections
- check all sources and colleciton shave identifier
- check all colleciton supported sources exist
- collection.Base Init is called
*/
// how may rows to write in each JSONL file
// TODO configure?
const JSONLChunkSize = 1000

// Base should be embedded in all [TailpipePlugin] implementations
type Base struct {
	observable.Base

	// row buffer keyed by execution id
	// each row buffer is used to write a JSONL file
	rowBufferMap map[string][]any
	// mutex for row buffer map AND rowCountMap
	rowBufferLock sync.RWMutex

	// map of row counts keyed by execution id
	rowCountMap map[string]int

	// map of collection schemas
	schemaMap             schema.SchemaMap
	writer                ChunkWriter
	artifactMapperFactory map[string]func() artifact.Mapper

	// mapps of the various registsred types
	collectionFactory     map[string]func() Collection
	sourceFactory         map[string]func() row_source.RowSource
	artifactLoaderFactory map[string]func() artifact.Loader
	artifactSourceFactory map[string]func() artifact.Source
}

// Init implements [plugin.TailpipePlugin]
func (b *Base) Init(context.Context) error {
	// if the plugin overrides this function it must call the base implementation
	// TODO #validation if overriden by plugin implementation, we need a way to validate this has been called
	b.rowBufferMap = make(map[string][]any)
	b.rowCountMap = make(map[string]int)

	// register the row sources provided by the sdk
	b.registerCommonRowSources()
	return nil
}

func (b *Base) Collect(ctx context.Context, req *proto.CollectRequest) error {
	log.Println("[INFO] Collect")

	// create writer
	b.writer = NewJSONLWriter(req.OutputPath)

	go func() {
		// create context containing execution id
		ctx = context_values.WithExecutionId(ctx, req.ExecutionId)

		if err := b.doCollect(ctx, req); err != nil {
			slog.Error("doCollect failed", "error", err)
			b.OnCompleted(ctx, req.ExecutionId, nil, err)
		}
	}()

	return nil
}

// Shutdown is called by Serve when the plugin exits
func (b *Base) Shutdown(context.Context) error {
	return nil
}

// GetSchema implements TailpipePlugin
func (b *Base) GetSchema() schema.SchemaMap {
	return b.schemaMap
}

func (b *Base) doCollect(ctx context.Context, req *proto.CollectRequest) error {
	// try to get the collection
	col, err := b.createCollection(ctx, req)
	if err != nil {
		return err
	}

	// add ourselves as an observer
	if err := col.AddObserver(b); err != nil {
		// TODO #err handle error
		slog.Error("add observer error", "error", err)
	}

	// signal we have started
	// signal we have started
	if err := b.OnStarted(ctx, req.ExecutionId); err != nil {
		return fmt.Errorf("error signalling started: %w", err)
	}

	// tell the collection to start collecting - this is a blocking call
	pagingData, err := col.Collect(ctx, req)

	// signal we have completed - pass error if there was one
	return b.OnCompleted(ctx, req.ExecutionId, pagingData, err)
}

func (b *Base) createCollection(ctx context.Context, req *proto.CollectRequest) (Collection, error) {
	// get the registered constructor for the collection
	ctor, ok := b.collectionFactory[req.CollectionData.Type]
	if !ok {
		return nil, fmt.Errorf("collection not found: %s", req.CollectionData.Type)
	}

	// create the collection
	col := ctor()

	//  register the collection implementation with the base struct (Before  calling Init)
	type BaseCollection interface{ RegisterImpl(Collection) }
	baseCol, ok := col.(BaseCollection)
	if !ok {
		return nil, fmt.Errorf("collection implementation must embed collection.Base")
	}
	baseCol.RegisterImpl(col)

	// convert req into collectionConfigData and sourceConfigData
	collectionConfigData := hcl.DataFromProto(req.CollectionData)
	sourceConfigData := hcl.DataFromProto(req.SourceData)

	// get any source options defined by colleciton for this source type
	sourceOpts := col.GetSourceOptions(req.SourceData.Type)

	// initialise the collection (passing ourselves as the 'sourceFactory`
	err := col.Init(ctx, b, collectionConfigData, sourceConfigData, sourceOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise collection: %w", err)
	}

	return col, nil
}

func (b *Base) OnCompleted(ctx context.Context, executionId string, pagingData paging.Data, err error) error {
	// tell our write to write any remaining rows

	// get row count and the rows in the buffers
	b.rowBufferLock.Lock()
	rowCount := b.rowCountMap[executionId]
	rowsToWrite := b.rowBufferMap[executionId]
	b.rowBufferMap[executionId] = nil
	b.rowCountMap[executionId] = 0
	b.rowBufferLock.Unlock()

	// tell our write to write any remaining rows
	if len(rowsToWrite) > 0 {
		if err := b.writeChunk(ctx, rowCount, rowsToWrite, pagingData); err != nil {
			slog.Error("failed to write final chunk", "error", err)
			return fmt.Errorf("failed to write final chunk: %w", err)
		}
	}

	// notify observers of completion
	// figure out the number of chunks written, including partial chunks
	chunksWritten := int(rowCount / JSONLChunkSize)
	if rowCount%JSONLChunkSize > 0 {
		chunksWritten++
	}

	return b.NotifyObservers(ctx, events.NewCompletedEvent(executionId, rowCount, chunksWritten, err))
}

func (b *Base) registerCommonRowSources() {
	b.RegisterSources(row_source.NewArtifactRowSource)
}
