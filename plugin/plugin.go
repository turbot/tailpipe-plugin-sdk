package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log"
	"log/slog"
	"sync"
)

// how may rows to write in each JSONL file
// TODO configure? https://github.com/turbot/tailpipe-plugin-sdk/issues/18
const JSONLChunkSize = 1000

// Plugin should be created via NewPlugin method.
type Plugin struct {
	observable.ObservableBase

	// row buffer keyed by execution id
	// each row buffer is used to write a JSONL file
	rowBufferMap map[string][]any
	// mutex for row buffer map AND rowCountMap
	rowBufferLock sync.RWMutex

	// map of row counts keyed by execution id
	rowCountMap map[string]int

	writer ChunkWriter

	identifier string
}

// NewPlugin creates a new plugin instance with the given identifier.
func NewPlugin(identifier string) *Plugin {
	return &Plugin{identifier: identifier}
}

// Identifier returns the plugin name
func (b *Plugin) Identifier() string {
	return b.identifier
}

// Init implements [plugin.TailpipePlugin]
func (b *Plugin) Init(context.Context) error {
	// if the plugin overrides this function it must call the base implementation
	b.rowBufferMap = make(map[string][]any)
	b.rowCountMap = make(map[string]int)
	return nil
}

// initialized returns true if the plugin has been initialized
func (b *Plugin) initialized() bool {
	return b.rowBufferMap != nil
}

func (b *Plugin) Collect(ctx context.Context, req *proto.CollectRequest) error {
	log.Println("[INFO] Collect")

	// create writer
	b.writer = NewJSONLWriter(req.OutputPath)

	go func() {
		// create context containing execution id
		ctx = context_values.WithExecutionId(ctx, req.ExecutionId)

		if err := b.doCollect(ctx, req); err != nil {
			slog.Error("doCollect failed", "error", err)
			b.OnCompleted(ctx, req.ExecutionId, nil, nil, err)
		}
	}()

	return nil
}

// Shutdown is called by Serve when the plugin exits
func (b *Plugin) Shutdown(context.Context) error {
	return nil
}

// GetSchema implements TailpipePlugin
func (b *Plugin) GetSchema() schema.SchemaMap {
	// ask the collection factory
	return table.Factory.GetSchema()
}

// Base returns the base instance - used for validation testing
func (b *Plugin) Base() *Plugin {
	return b
}

func (b *Plugin) OnCompleted(ctx context.Context, executionId string, collectionState json.RawMessage, timing types.TimingCollection, err error) error {
	// get row count and the rows in the buffers
	b.rowBufferLock.Lock()
	rowCount := b.rowCountMap[executionId]
	rowsToWrite := b.rowBufferMap[executionId]
	b.rowBufferMap[executionId] = nil
	b.rowCountMap[executionId] = 0
	b.rowBufferLock.Unlock()

	// tell our write to write any remaining rows
	if len(rowsToWrite) > 0 {
		if err := b.writeChunk(ctx, rowCount, rowsToWrite, collectionState); err != nil {
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

	return b.NotifyObservers(ctx, events.NewCompletedEvent(executionId, rowCount, chunksWritten, timing, err))
}

func (b *Plugin) doCollect(ctx context.Context, req *proto.CollectRequest) error {
	// ask the factory to create the table
	// - this will configure the requested source
	table, err := table.Factory.GetTable(ctx, req)
	if err != nil {
		return err
	}

	// add ourselves as an observer
	if err := table.AddObserver(b); err != nil {
		// TODO #error handle error
		slog.Error("add observer error", "error", err)
	}

	// signal we have started
	if err := b.OnStarted(ctx, req.ExecutionId); err != nil {
		return fmt.Errorf("error signalling started: %w", err)
	}

	// tell the collection to start collecting - this is a blocking call
	collectionState, err := table.Collect(ctx, req)

	timing := table.GetTiming()

	// signal we have completed - pass error if there was one
	return b.OnCompleted(ctx, req.ExecutionId, collectionState, timing, err)
}
