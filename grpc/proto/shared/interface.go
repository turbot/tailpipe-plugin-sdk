package shared

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	"github.com/turbot/tailpipe-plugin/grpc/proto"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
	// This isn't required when using VersionedPlugins
	ProtocolVersion:  1,
	MagicCookieKey:   "TAILPIPE_PLUGIN",
	MagicCookieValue: "tailpipe plugin",
}

// todo build dynamically as per steampipe
// PluginMap is the map of plugins we can dispense.
var PluginMap = map[string]plugin.Plugin{
	"tailpipe_plugin": &TailpipeGRPCPlugin{},
}

// TailpipePluginServer is the service interface that we're exposing as a plugin.
type TailpipePluginWrapperServer interface {
	GetSchema() (*proto.GetSchemaResponse, error)
	AddObserver(stream proto.TailpipePlugin_AddObserverServer) error

	// TODO add collect params
	Collect(req *proto.CollectRequest) error
}

// TailpipePlugin is the client interface that we're exposing as a plugin.
type TailpipePluginWrapperClient interface {
	GetSchema() (*proto.GetSchemaResponse, error)
	AddObserver() (proto.TailpipePlugin_AddObserverClient, error)
	// TODO add collect params
	Collect(req *proto.CollectRequest) error
}

// TailpipeGRPCPlugin is the implementation of plugin.GRPCPlugin so we can serve/consume this.
type TailpipeGRPCPlugin struct {
	// GRPCPlugin must still implement the Plugin interface
	plugin.Plugin
	// Concrete implementation, written in Go. This is only used for plugins
	// that are written in Go.
	Impl TailpipePluginWrapperServer
}

func (p *TailpipeGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterTailpipePluginServer(s, &TailpipePluginServerWrapper{Impl: p.Impl})
	return nil
}

func (p *TailpipeGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &TailpipePluginClientWrapper{client: proto.NewTailpipePluginClient(c)}, nil
}
