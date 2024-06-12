package shared

import (
	"context"
	"github.com/turbot/tailpipe-plugin/grpc/proto"
	"log"
)

// TailpipePluginClientWrapper is an implementation of TailpipePlugin that talks over GRPC.
type TailpipePluginClientWrapper struct{ client proto.TailpipePluginClient }

func (c TailpipePluginClientWrapper) GetSchema() (*proto.GetSchemaResponse, error) {
	return c.client.GetSchema(context.Background(), &proto.GetSchemaRequest{})
}

func (c TailpipePluginClientWrapper) AddObserver() (proto.TailpipePlugin_AddObserverClient, error) {
	return c.client.AddObserver(context.Background(), &proto.AddObserverRequest{})
}
func (c TailpipePluginClientWrapper) Collect(req *proto.CollectRequest) error {
	_, err := c.client.Collect(context.Background(), req)
	return err
}

// TailpipePluginServerWrapper is the gRPC server that TailpipePluginClient talks to.
type TailpipePluginServerWrapper struct {
	proto.UnimplementedTailpipePluginServer
	// This is the real implementation
	Impl TailpipePluginWrapperServer
}

func (s TailpipePluginServerWrapper) GetSchema(_ context.Context, _ *proto.GetSchemaRequest) (*proto.GetSchemaResponse, error) {
	log.Println("[INFO] TailpipePluginServerWrapper AddObserver")

	return s.Impl.GetSchema()
}

func (s TailpipePluginServerWrapper) AddObserver(_ *proto.AddObserverRequest, server proto.TailpipePlugin_AddObserverServer) error {
	log.Println("[INFO] TailpipePluginServerWrapper AddObserver")

	return s.Impl.AddObserver(server)
}
func (s TailpipePluginServerWrapper) Collect(_ context.Context, req *proto.CollectRequest) (*proto.Empty, error) {
	return nil, s.Impl.Collect(req)
}
