package shared

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"log"
)

// TailpipePluginClientWrapper is an implementation of TailpipePlugin that talks over GRPC.
type TailpipePluginClientWrapper struct{ client proto.TailpipePluginClient }

func (c TailpipePluginClientWrapper) AddObserver() (proto.TailpipePlugin_AddObserverClient, error) {
	return c.client.AddObserver(context.Background(), &proto.AddObserverRequest{})
}
func (c TailpipePluginClientWrapper) Collect(req *proto.CollectRequest) (*proto.CollectResponse, error) {
	return c.client.Collect(context.Background(), req)
}

func (c TailpipePluginClientWrapper) Describe() (*proto.DescribeResponse, error) {
	return c.client.Describe(context.Background(), &proto.DescribeRequest{})
}

// TailpipePluginServerWrapper is the gRPC server that TailpipePluginClient talks to.
type TailpipePluginServerWrapper struct {
	proto.UnimplementedTailpipePluginServer
	// This is the real implementation
	Impl TailpipePluginServer
}

func (s TailpipePluginServerWrapper) AddObserver(_ *proto.AddObserverRequest, server proto.TailpipePlugin_AddObserverServer) error {
	return s.Impl.AddObserver(server)
}

func (s TailpipePluginServerWrapper) Collect(_ context.Context, req *proto.CollectRequest) (*proto.CollectResponse, error) {
	// validate the request
	if err := req.Validate(); err != nil {
		return nil, err
	}

	// create a new context as the collect process will live onm after this call has returned, and this context will be cancelled on return from the call
	return s.Impl.Collect(context.Background(), req)
}

func (s TailpipePluginServerWrapper) Describe(_ context.Context, _ *proto.DescribeRequest) (*proto.DescribeResponse, error) {
	log.Println("[INFO] TailpipePluginServerWrapper AddObserver")

	return s.Impl.Describe()
}
