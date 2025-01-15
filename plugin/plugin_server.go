package plugin

import (
	"context"
	"log/slog"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"github.com/turbot/tailpipe-plugin-sdk/logging"
)

// PluginServer is a wrapper for the actual plugin
// - this allows us to map between the GRPC interface and the interface implemented by the plugin
// this is use in particular for AddObserver which has a different signature -
// this wrapping enables us to define plugin-scoped events which are supported by all plugin components which do not
// need to know about the GRPC interface and with no corresponding protobuff events
type PluginServer struct {
	impl TailpipePlugin
}

func NewPluginServer(opts *ServeOpts) (*PluginServer, error) {
	// retrieve the plugin from the opts
	p, err := opts.PluginFunc()
	if err != nil {
		return nil, err
	}

	s := &PluginServer{
		impl: p,
	}
	return s, nil
}

func (s PluginServer) AddObserver(stream proto.TailpipePlugin_AddObserverServer) error {
	// wrap the stream in an ObserverWrapper to map between the plugin events and proto events
	err := s.impl.AddObserver(NewObserverWrapper(stream))
	if err != nil {
		return err
	}

	// TODO do we need a remove observer function, in which case this could wait on a waitgroup associated with the observer? https://github.com/turbot/tailpipe-plugin-sdk/issues/19
	// hold stream open
	<-stream.Context().Done()

	return stream.Context().Err()
}

// Describe returns the schema for the plugin
func (s PluginServer) Describe() (*proto.DescribeResponse, error) {
	describeResponse, err := s.impl.Describe()
	if err != nil {
		return nil, err
	}

	// convert the response to proto
	resp := describeResponse.ToProto()

	return resp, nil
}

func (s PluginServer) Collect(ctx context.Context, req *proto.CollectRequest) (*proto.CollectResponse, error) {
	fromTime, schema, err := s.impl.Collect(ctx, req)
	if err != nil {
		return nil, err
	}
	// build response
	var resp = &proto.CollectResponse{
		ExecutionId: req.ExecutionId,
		Schema:      schema.ToProto(),
		FromTime:    fromTime.ToProto(),
	}
	return resp, nil
}

func (s PluginServer) InitSource(ctx context.Context, req *proto.InitSourceRequest) (*proto.InitSourceResponse, error) {
	fromTime, err := s.impl.InitSource(ctx, req)
	if err != nil {
		return nil, err
	}

	return &proto.InitSourceResponse{
		FromTime: fromTime.ToProto(),
	}, nil
}

func (s PluginServer) SaveCollectionState(ctx context.Context, _ *proto.Empty) (*proto.Empty, error) {
	err := s.impl.SaveCollectionState(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}

func (s PluginServer) CloseSource(ctx context.Context, _ *proto.Empty) (*proto.Empty, error) {
	err := s.impl.CloseSource(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}

func (s PluginServer) SourceCollect(ctx context.Context, req *proto.SourceCollectRequest) (*proto.Empty, error) {
	err := s.impl.SourceCollect(ctx, req)
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}

func (s PluginServer) GetSourceTiming(ctx context.Context, _ *proto.Empty) (*proto.GetSourceTimingResponse, error) {
	sourceTiming, err := s.impl.GetSourceTiming(ctx)
	if err != nil {
		return nil, err
	}
	// convert the response to proto
	resp := &proto.GetSourceTimingResponse{
		Timing: events.TimingCollectionToProto(sourceTiming),
	}
	return resp, nil
}

func (s PluginServer) Serve() error {
	// use plugin provided in opts
	ctx := context.Background()

	// initialize logger
	logging.Initialize(s.impl.Identifier())

	// initialise the plugin
	if err := s.impl.Init(ctx); err != nil {
		return err
	}
	// shutdown the plugin when done
	defer func() {
		if err := s.impl.Shutdown(ctx); err != nil {
			// TODO #error what to do with this error?
			slog.Error("failed to shutdown plugin", "error", err)
		}
	}()

	if _, found := os.LookupEnv("TAILPIPE_PPROF"); found {
		setupPprof()
	}

	pluginMap := map[string]plugin.Plugin{
		s.impl.Identifier(): &shared.TailpipeGRPCPlugin{Impl: s},
	}
	plugin.Serve(&plugin.ServeConfig{
		Plugins:         pluginMap,
		GRPCServer:      newGRPCServer,
		HandshakeConfig: shared.Handshake,
		// disable server logging
		Logger: hclog.New(&hclog.LoggerOptions{Level: hclog.Off}),
	})
	return nil
}
