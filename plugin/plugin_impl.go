package plugin

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// PluginImpl should be created via NewPluginImpl method.
type PluginImpl struct {
	observable.ObservableImpl

	identifier string

	// if the plugin is being used as a source, this will be set
	source row_source.RowSource
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
	//initialise the table factory
	// this converts the array of table constructors to a map of table constructors
	// and populates the table schemas
	return table.Factory.Init()
}

// initialized returns true if the plugin has been initialized
func (p *PluginImpl) initialized() bool {
	return table.Factory.Initialized()
}

func (p *PluginImpl) Collect(ctx context.Context, req *proto.CollectRequest) (*schema.RowSchema, error) {
	slog.Info("Collect")

	// create context containing execution id
	ctx = context_values.WithExecutionId(ctx, req.ExecutionId)

	// map req to our internal type
	collectRequest, err := types.CollectRequestFromProto(req)
	if err != nil {
		slog.Error("CollectRequestFromProto failed", "error", err)

		return nil, err
	}

	// ask the factory to create the collector
	// - this will configure the requested source
	collector, err := table.Factory.GetCollector(collectRequest)
	if err != nil {
		return nil, err
	}

	// initialise the collector
	if err := collector.Init(ctx, collectRequest); err != nil {
		return nil, err
	}

	// add ourselves as an observer
	if err := collector.AddObserver(p); err != nil {
		slog.Error("add observer error", "error", err)
		return nil, err
	}

	// signal we have started
	if err := p.OnStarted(ctx, req.ExecutionId); err != nil {
		err := fmt.Errorf("error signalling started: %w", err)
		_ = p.OnCompleted(ctx, req.ExecutionId, 0, 0, nil, err)
	}

	go func() {
		// tell the collection to start collecting - this is a blocking call
		rowCount, chunksWritten, err := collector.Collect(ctx)
		var timing types.TimingCollection
		if err == nil {
			timing, err = collector.GetTiming()
		}

		// signal we have completed - pass error if there was one
		_ = p.OnCompleted(ctx, req.ExecutionId, rowCount, chunksWritten, timing, err)
	}()

	// return the schema (if available - this may be nil for dynamic tables, in which case the CLI will infer the schema)
	return collector.GetSchema()
}

// Describe implements TailpipePlugin
func (p *PluginImpl) Describe() (DescribeResponse, error) {
	schemas, err := table.Factory.GetSchema()
	if err != nil {
		return DescribeResponse{}, err
	}
	sources, err := row_source.Factory.DescribeSources()
	if err != nil {
		return DescribeResponse{}, err
	}
	return DescribeResponse{
		Schemas: schemas,
		Sources: sources,
	}, nil
}

// InitSource is called to initialise the source when this plugin is being used as a source
func (p *PluginImpl) InitSource(ctx context.Context, req *proto.InitSourceRequest) error {
	// ask factory to create and initialise the source for us
	initSourceRequest, err := types.InitSourceRequestFromProto(req)
	if err != nil {
		return err
	}

	source, err := row_source.Factory.GetRowSource(ctx, initSourceRequest.SourceData, initSourceRequest.ConnectionData)
	if err != nil {
		return err
	}

	// this must be an artifact source
	as, ok := source.(artifact_source.ArtifactSource)
	if !ok {
		return fmt.Errorf("source is not an artifact source")
	}

	// set the loader to a null loader to avoid this plugin instance loading/processing the downloaded artifacts
	// (the calling plugin will do that)
	as.SetLoader(artifact_loader.NewNullLoader())

	// add ourselves as observer to the source
	err = as.AddObserver(p)
	if err != nil {
		return err
	}
	p.source = as

	return nil
}

func (p *PluginImpl) CloseSource(_ context.Context) error {
	if p.source == nil {
		return fmt.Errorf("source not initialised")
	}
	return p.source.Close()
}

func (p *PluginImpl) SourceCollect(ctx context.Context, req *proto.SourceCollectRequest) error {
	if p.source == nil {
		return fmt.Errorf("source not initialised")
	}
	// create context containing execution id
	ctx = context_values.WithExecutionId(ctx, req.ExecutionId)

	err := p.source.Collect(ctx)
	if err != nil {
		p.NotifyError(ctx, req.ExecutionId, err)
	} else {
		timing, err := p.GetSourceTiming(ctx)
		if err != nil {
			slog.Error("error getting source timing", "error", err)
		}
		notifyError := p.NotifyObservers(ctx, events.NewSourceCompletedEvent(req.ExecutionId, timing, err))
		if notifyError != nil {
			p.NotifyError(ctx, req.ExecutionId, notifyError)
		}
	}
	return err
}

func (p *PluginImpl) GetSourceTiming(ctx context.Context) (types.TimingCollection, error) {
	if p.source == nil {
		return types.TimingCollection{}, fmt.Errorf("source not initialised")
	}
	return p.source.GetTiming()
}

// Shutdown is called by Serve when the plugin exits
func (p *PluginImpl) Shutdown(context.Context) error {
	return nil
}

// Impl returns the base instance - used for validation testing
func (p *PluginImpl) Impl() *PluginImpl {
	return p
}

func (p *PluginImpl) OnCompleted(ctx context.Context, executionId string, rowCount int, chunksWritten int, timing types.TimingCollection, err error) error {
	return p.NotifyObservers(ctx, events.NewCompletedEvent(executionId, rowCount, chunksWritten, timing, err))
}
