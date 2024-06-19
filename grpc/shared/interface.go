package shared

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"google.golang.org/grpc"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
	// This isn't required when using VersionedPlugins
	ProtocolVersion:  1,
	MagicCookieKey:   "TAILPIPE_PLUGIN",
	MagicCookieValue: "tailpipe plugin",
}

// TailpipePluginServer is the service interface that we're exposing as a plugin.
type TailpipePluginServer interface {
	AddObserver(stream proto.TailpipePlugin_AddObserverServer) error
	Collect(req *proto.CollectRequest) error
}

// TailpipePluginClient is the client interface that we're exposing as a plugin.
type TailpipePluginClient interface {
	AddObserver() (proto.TailpipePlugin_AddObserverClient, error)
	Collect(req *proto.CollectRequest) error
}

// TailpipeGRPCPlugin is the implementation of plugin.GRPCPlugin so we can serve/consume this.
type TailpipeGRPCPlugin struct {
	// GRPCPlugin must still implement the Plugin interface
	plugin.Plugin
	// Concrete implementation, written in Go. This is only used for plugins
	// that are written in Go.
	Impl TailpipePluginServer
}

func (p *TailpipeGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterTailpipePluginServer(s, &TailpipePluginServerWrapper{Impl: p.Impl})
	return nil
}

func (p *TailpipeGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &TailpipePluginClientWrapper{client: proto.NewTailpipePluginClient(c)}, nil
}
