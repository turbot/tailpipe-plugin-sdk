package plugin

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

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

	// map of Collection constructors
	collectionFactory map[string]func() Collection
	// map of RowSource constructors
	sourceFactory map[string]func() RowSource

	// map of collection schemas
	schemaMap schema.SchemaMap
	writer    ChunkWriter
}

// Init implements [plugin.TailpipePlugin]
// if the plugin overrides this function it must call the base implementation
func (b *Base) Init(context.Context) error {
	// TODO #validation if overriden by plugin implementation, we need a way to validate this has been called

	b.rowBufferMap = make(map[string][]any)
	b.rowCountMap = make(map[string]int)
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
	ctor, ok := b.collectionFactory[req.CollectionType]
	if !ok {
		return nil, fmt.Errorf("collection not found: %s", req.CollectionType)
	}

	// create the collection
	col := ctor()

	//  register the collection implementation with the base struct (before init)
	type BaseCollection interface{ RegisterImpl(Collection) }
	baseCol, ok := col.(BaseCollection)
	if !ok {
		return nil, fmt.Errorf("collection implementation must embed collection.Base")
	}
	baseCol.RegisterImpl(col)

	// initialise the collection
	if err := col.Init(ctx, req.CollectionConfig); err != nil {
		return nil, fmt.Errorf("failed to initialise collection: %w", err)
	}

	return col, nil
}

// Shutdown implements Tailpipe It is called by Serve when the plugin exits
func (b *Base) Shutdown(context.Context) error {
	return nil
}

// GetSchema implements TailpipePlugin
func (b *Base) GetSchema() schema.SchemaMap {
	return b.schemaMap
}

func (b *Base) getRowCount(executionId string) (int, int) {
	// get rowcount
	b.rowBufferLock.RLock()
	rowCount := b.rowCountMap[executionId]
	b.rowBufferLock.RUnlock()

	// notify observers of completion
	// figure out the number of chunks written, including partial chunks
	chunksWritten := int(rowCount / JSONLChunkSize)
	if rowCount%JSONLChunkSize > 0 {
		chunksWritten++
	}
	return rowCount, chunksWritten
}

// RegisterSources registers RowSource implementations
// is should be called by a plugin implementation to register the sources it provides
// it is also called by the base implementation to register the sources the SDK provides
func (b *Base) RegisterSources(sourceFunc ...func() RowSource) error {
	// create the maps if necessary
	if b.sourceFactory == nil {
		b.sourceFactory = make(map[string]func() RowSource)
	}

	errs := make([]error, 0)
	for _, ctor := range sourceFunc {
		// create an instance of the source to get the identifier
		c := ctor()
		// register the collection
		b.sourceFactory[c.Identifier()] = ctor
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (b *Base) RegisterCollections(collectionFunc ...func() Collection) error {
	// create the maps
	b.collectionFactory = make(map[string]func() Collection)
	b.schemaMap = make(map[string]*schema.RowSchema)

	errs := make([]error, 0)
	for _, ctor := range collectionFunc {
		// create an instance of the collection to get the identifier
		c := ctor()
		// register the collection
		b.collectionFactory[c.Identifier()] = ctor

		// get the schema for the collection row type
		rowStruct := c.GetRowSchema()
		s, err := schema.SchemaFromStruct(rowStruct)
		if err != nil {
			errs = append(errs, err)
		}

		b.schemaMap[c.Identifier()] = s
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
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

	// get row count
	rowCount, chunksWritten := b.getRowCount(executionId)

	return b.NotifyObservers(ctx, events.NewCompletedEvent(executionId, rowCount, chunksWritten, err))
}
