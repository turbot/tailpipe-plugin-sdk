package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// how may rows to write in each JSONL file
// TODO configure? https://github.com/turbot/tailpipe-plugin-sdk/issues/18
const JSONLChunkSize = 1000

// PluginImpl should be created via NewPluginImpl method.
type PluginImpl struct {
	observable.ObservableImpl

	// row buffer keyed by execution id
	// each row buffer is used to write a JSONL file
	rowBufferMap map[string][]any
	// mutex for row buffer map AND rowCountMap
	rowBufferLock sync.RWMutex

	// map of row counts keyed by execution id
	rowCountMap map[string]int

	writer ChunkWriter

	identifier     string
	connectionFunc func() parse.Config
}

// NewPluginImpl creates a new PluginImpl instance with the given identifier.
func NewPluginImpl(identifier string, connectionFunc func() parse.Config) PluginImpl {
	return PluginImpl{
		identifier:     identifier,
		connectionFunc: connectionFunc,
	}
}

// Identifier returns the plugin name
func (p *PluginImpl) Identifier() string {
	return p.identifier
}

// Init implements [plugin.TailpipePlugin]
func (p *PluginImpl) Init(context.Context) error {
	// if the plugin overrides this function it must call the base implementation
	p.rowBufferMap = make(map[string][]any)
	p.rowCountMap = make(map[string]int)
	return nil
}

// initialized returns true if the plugin has been initialized
func (p *PluginImpl) initialized() bool {
	return p.rowBufferMap != nil
}

func (p *PluginImpl) Collect(ctx context.Context, req *proto.CollectRequest) error {
	log.Println("[INFO] Collect")

	// create writer
	p.writer = NewJSONLWriter(req.OutputPath)

	go func() {
		// create context containing execution id
		ctx = context_values.WithExecutionId(ctx, req.ExecutionId)

		// map req to our internal type
		collectRequest, err := types.CollectRequestFromProto(req)
		if err != nil {
			slog.Error("CollectRequestFromProto failed", "error", err)
			_ = p.OnCompleted(ctx, req.ExecutionId, nil, nil, err)
			return
		}
		err = p.doCollect(ctx, collectRequest)
		if err != nil {
			slog.Error("doCollect failed", "error", err)
			_ = p.OnCompleted(ctx, req.ExecutionId, nil, nil, err)
		}
	}()

	return nil
}

// Shutdown is called by Serve when the plugin exits
func (p *PluginImpl) Shutdown(context.Context) error {
	return nil
}

// GetSchema implements TailpipePlugin
func (p *PluginImpl) GetSchema() schema.SchemaMap {
	// ask the collection factory
	return table.Factory.GetSchema()
}

// GetConnectionSchema implements table.ConnectionSchemaProvider
func (p *PluginImpl) GetConnectionSchema() parse.Config {
	// instantiate the connection config
	return p.connectionFunc()
}

// Base returns the base instance - used for validation testing
func (p *PluginImpl) Impl() *PluginImpl {
	return p
}

func (p *PluginImpl) OnCompleted(ctx context.Context, executionId string, collectionState json.RawMessage, timing types.TimingCollection, err error) error {
	// get row count and the rows in the buffers
	p.rowBufferLock.Lock()
	rowCount := p.rowCountMap[executionId]
	rowsToWrite := p.rowBufferMap[executionId]
	p.rowBufferMap[executionId] = nil
	p.rowCountMap[executionId] = 0
	p.rowBufferLock.Unlock()

	// tell our write to write any remaining rows
	if len(rowsToWrite) > 0 {
		if err := p.writeChunk(ctx, rowCount, rowsToWrite, collectionState); err != nil {
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

	return p.NotifyObservers(ctx, events.NewCompletedEvent(executionId, rowCount, chunksWritten, timing, err))
}

func (p *PluginImpl) doCollect(ctx context.Context, req *types.CollectRequest) error {
	// ask the factory to create the table
	// - this will configure the requested source
	t, err := table.Factory.GetTable(ctx, req, p)
	if err != nil {
		return err
	}

	// add ourselves as an observer
	if err := t.AddObserver(p); err != nil {
		// TODO #error handle error
		slog.Error("add observer error", "error", err)
	}

	// signal we have started
	if err := p.OnStarted(ctx, req.ExecutionId); err != nil {
		return fmt.Errorf("error signalling started: %w", err)
	}

	// tell the collection to start collecting - this is a blocking call
	collectionState, err := t.Collect(ctx, req)

	timing := t.GetTiming()

	// signal we have completed - pass error if there was one
	return p.OnCompleted(ctx, req.ExecutionId, collectionState, timing, err)
}
