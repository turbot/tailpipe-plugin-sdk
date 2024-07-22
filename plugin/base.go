package plugin

import (
	"context"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"log"
	"log/slog"
	"sync"
)

// how may rows to write in each JSONL file
// TODO configure?
const JSONLChunkSize = 10000

// Base should be embedded in all tailpipe plugin implementations
type Base struct {
	observable.Base

	// row buffer keyed by execution id
	// each row buffer is used to write a JSONL file
	rowBufferMap map[string][]any
	// mutex for row buffer map AND rowCountMap
	rowBufferLock sync.RWMutex

	// map of row counts keyed by execution id
	rowCountMap map[string]int

	// map of collection constructors
	collectionFactory map[string]func() Collection

	// map of collection schemas
	schemaMap schema.SchemaMap
	writer    ChunkWriter
}

// Init implements Tailpipe It is called by Serve when the plugin is started
// if the plugin overrides this function it must call the base implementation
func (b *Base) Init(context.Context) error {
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
			// TODO #err handle error
			slog.Error("doCollect failed", "error", err)
		}
	}()

	return nil
}

func (b *Base) doCollect(ctx context.Context, req *proto.CollectRequest) error {
	// try to get the collection
	col, err := b.createCollection(ctx, req.CollectionName, req.Config)
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
	err = col.Collect(ctx, req)

	// signal we have completed - pass error if there was one
	return b.OnCompleted(ctx, req.ExecutionId, err)
}

func (b *Base) createCollection(ctx context.Context, collectionName string, config []byte) (Collection, error) {
	ctor, ok := b.collectionFactory[collectionName]
	if !ok {
		return nil, fmt.Errorf("collection not found: %s", collectionName)
	}
	col := ctor()

	if err := col.Init(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to initialise collection: %w", err)
	}

	// now register the collection implemtnation with the base struct
	type BaseCollection interface{ RegisterImpl(Collection) }
	base, ok := col.(BaseCollection)
	if !ok {
		return nil, fmt.Errorf("collection implementation must embed collection.Base")
	}
	base.RegisterImpl(col)

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

func (b *Base) RegisterCollections(collectionFunc ...func() Collection) error {
	// create the maps
	b.collectionFactory = make(map[string]func() Collection)
	b.schemaMap = make(map[string]*schema.RowSchema)

	commonSchema, err := schema.SchemaFromStruct(enrichment.CommonFields{})
	if err != nil {
		return fmt.Errorf("failed to create schema for common fields: %w", err)
	}
	errs := make([]error, 0)
	for _, ctor := range collectionFunc {
		c := ctor()
		// register the collection
		b.collectionFactory[c.Identifier()] = ctor

		// get the schema for the collection row type
		rowStruct := c.GetRowStruct()
		s, err := schema.SchemaFromStruct(rowStruct)
		if err != nil {
			errs = append(errs, err)
		}
		// merge in the common schema
		s.Merge(commonSchema)
		b.schemaMap[c.Identifier()] = s
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil

}

func (b *Base) OnCompleted(ctx context.Context, executionId string, err error) error {
	// get row count
	rowCount, chunksWritten := b.getRowCount(executionId)

	return b.NotifyObservers(ctx, events.NewCompletedEvent(executionId, rowCount, chunksWritten, err))
}
