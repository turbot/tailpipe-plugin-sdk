package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"github.com/turbot/tailpipe-plugin-sdk/logging"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

// PluginServer is a wrapper for the actual plugin
// - this allows us to map between the GRPC interface and the interface implemented by the plugin
// this is use in particular for AddObserver which has a different signature -
// this wrapping enables us to define plugin-scoped events which are supported by all plugin components which do not
// need to know about the GRPC interface and with no corresponding protobuff events
type PluginServer struct {
	impl TailpipePlugin
}

// ObserverWrapper mapsd between proto Observer and the plugin Observer
type ObserverWrapper struct {
	protoObserver proto.TailpipePlugin_AddObserverServer
}

// ctor
func NewObserverWrapper(protoObserver proto.TailpipePlugin_AddObserverServer) ObserverWrapper {
	return ObserverWrapper{protoObserver: protoObserver}
}

// Notify implements the Observer interface but sends to a proto stream
func (o ObserverWrapper) Notify(c context.Context, e events.Event) error {
	if p, ok := e.(events.ProtoEvent); ok {
		return o.protoObserver.Send(p.ToProto())
	}
	return fmt.Errorf("event %v does not implement ProtoEvent", e)
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

func (s PluginServer) Collect(ctx context.Context, req *proto.CollectRequest) error {
	// before collection, initialise the table factory
	// this converts trhe array of table constructors to a map of table constructors
	// and populates the table schemas
	if err := table.Factory.Init(); err != nil {
		return err
	}
	return s.impl.Collect(ctx, req)
}

// GetSchema returns the schema for the plugin
func (s PluginServer) GetSchema() (*proto.GetSchemaResponse, error) {
	schemaMap := s.impl.GetSchema()

	// convert the schema to proto

	resp := &proto.GetSchemaResponse{
		Schemas: schemaMap.ToProto(),
	}
	return resp, nil
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
