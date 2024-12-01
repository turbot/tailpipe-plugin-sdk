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
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
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

	identifier string
}

// NewPluginImpl creates a new PluginImpl instance with the given identifier.
func NewPluginImpl(identifier string) PluginImpl {
	return PluginImpl{
		identifier: identifier,
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

	//initialise the table factory
	// this converts the array of table constructors to a map of table constructors
	// and populates the table schemas
	return table.Factory.Init()
}

// initialized returns true if the plugin has been initialized
func (p *PluginImpl) initialized() bool {
	return p.rowBufferMap != nil
}

func (p *PluginImpl) Collect(ctx context.Context, req *proto.CollectRequest) (*schema.RowSchema, error) {
	log.Println("[INFO] Collect")

	// create writer
	p.writer = NewJSONLWriter(req.OutputPath)

	// map req to our internal type
	collectRequest, err := types.CollectRequestFromProto(req)
	if err != nil {
		slog.Error("CollectRequestFromProto failed", "error", err)

		return nil, err
	}

	// ask the factory to create the partition
	// - this will configure the requested source
	partition, err := table.Factory.GetPartition(collectRequest)
	if err != nil {
		return nil, err
	}

	// initialise the partition
	if err := partition.Init(ctx, collectRequest); err != nil {
		return nil, err
	}

	// get the schema
	// NOTE: must be before we start collecting
	// TODO make this part of init?
	schemaChan, err := partition.GetSchemaAsync()
	if err != nil {
		return nil, err
	}

	// add ourselves as an observer
	if err := partition.AddObserver(p); err != nil {
		slog.Error("add observer error", "error", err)
		return nil, err
	}

	// create context containing execution id
	ctx = context_values.WithExecutionId(ctx, req.ExecutionId)

	// signal we have started
	if err := p.OnStarted(ctx, req.ExecutionId); err != nil {
		err := fmt.Errorf("error signalling started: %w", err)
		_ = p.OnCompleted(ctx, req.ExecutionId, nil, nil, err)
	}

	go func() {
		// tell the collection to start collecting - this is a blocking call
		collectionState, err := partition.Collect(ctx)
		if err != nil {
			_ = p.OnCompleted(ctx, req.ExecutionId, nil, nil, err)
		}

		timing := partition.GetTiming()

		// signal we have completed - pass error if there was one
		_ = p.OnCompleted(ctx, req.ExecutionId, collectionState, timing, err)
	}()

	// wait for the schema
	tableSchema := <-schemaChan

	return tableSchema, nil
}

// Describe implements TailpipePlugin
func (p *PluginImpl) Describe() DescribeResponse {
	return DescribeResponse{
		Schemas: table.Factory.GetSchema(),
		Sources: row_source.Factory.DescribeSources(),
	}
}

// Shutdown is called by Serve when the plugin exits
func (p *PluginImpl) Shutdown(context.Context) error {
	return nil
}

// Impl returns the base instance - used for validation testing
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
