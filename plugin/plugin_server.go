package plugin

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"os"
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
func (o ObserverWrapper) Notify(e events.Event) error {
	return o.protoObserver.Send(e.ToProto())
}

func (s PluginServer) AddObserver(stream proto.TailpipePlugin_AddObserverServer) error {
	// wrap the stream in an ObserverWrapper to map between the plugin events and proto events
	err := s.impl.AddObserver(NewObserverWrapper(stream))
	if err != nil {
		return err
	}
	// hold stream open
	// TODO do we need a remove observer function, in which case this could wait on a waitgroup associated with the observer?
	select {}
	return nil
}

func (s PluginServer) Collect(req *proto.CollectRequest) error {
	return s.impl.Collect(req)
}

func NewPluginServer(opts *ServeOpts) *PluginServer {
	return &PluginServer{
		impl: opts.Plugin,
	}
}

func (s PluginServer) Serve() error {
	// use plugin provided in opts
	ctx := context.Background()

	//time.Sleep(10 * time.Second)

	// initialise the plugin
	if err := s.impl.Init(ctx); err != nil {
		return err
	}
	// shutdown the plugin when done
	defer s.impl.Shutdown(ctx)

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
	})
	return nil
}
