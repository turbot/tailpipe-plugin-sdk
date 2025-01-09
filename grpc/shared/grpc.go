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

func (c TailpipePluginClientWrapper) InitSource(req *proto.InitSourceRequest) (*proto.InitResponse, error) {
	return c.client.InitSource(context.Background(), req)
}

func (c TailpipePluginClientWrapper) CloseSource() (*proto.CloseSourceResponse, error) {
	return c.client.CloseSource(context.Background(), &proto.CloseSourceRequest{})
}

func (c TailpipePluginClientWrapper) SourceCollect(req *proto.SourceCollectRequest) (*proto.SourceCollectResponse, error) {
	return c.client.SourceCollect(context.Background(), req)
}

func (c TailpipePluginClientWrapper) GetSourceTiming() (*proto.GetSourceTimingResponse, error) {
	return c.client.GetSourceTiming(context.Background(), &proto.GetSourceTimingRequest{})
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

func (s TailpipePluginServerWrapper) InitSource(_ context.Context, req *proto.InitSourceRequest) (*proto.InitResponse, error) {
	return s.Impl.InitSource(context.Background(), req)
}

func (s TailpipePluginServerWrapper) CloseSource(_ context.Context, req *proto.CloseSourceRequest) (*proto.CloseSourceResponse, error) {
	return s.Impl.CloseSource(context.Background(), req)
}

func (s TailpipePluginServerWrapper) SourceCollect(_ context.Context, req *proto.SourceCollectRequest) (*proto.SourceCollectResponse, error) {
	return s.Impl.SourceCollect(context.Background(), req)
}

func (s TailpipePluginServerWrapper) GetSourceTiming(_ context.Context, req *proto.GetSourceTimingRequest) (*proto.GetSourceTimingResponse, error) {
	return s.Impl.GetSourceTiming(context.Background(), req)
}
