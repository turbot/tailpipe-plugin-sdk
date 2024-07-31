package plugin

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
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

	writer ChunkWriter
}

// Init implements [plugin.TailpipePlugin]
func (b *Base) Init(context.Context) error {
	// if the plugin overrides this function it must call the base implementation
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

// Shutdown is called by Serve when the plugin exits
func (b *Base) Shutdown(context.Context) error {
	return nil
}

// GetSchema implements TailpipePlugin
func (b *Base) GetSchema() schema.SchemaMap {
	// ask the collection factory
	return collection.Factory.GetSchema()
}

func (b *Base) OnCompleted(ctx context.Context, executionId string, pagingData paging.Data, err error) error {
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

func (b *Base) doCollect(ctx context.Context, req *proto.CollectRequest) error {
	// ask the factory to create the collection
	// - this will configure the requested source
	col, err := collection.Factory.GetCollection(ctx, req)
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
