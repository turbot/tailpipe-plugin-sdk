package shared

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// TailpipePluginClientWrapper is an implementation of TailpipePluginClient that talks over GRPC.
type TailpipePluginClientWrapper struct{ client proto.TailpipePluginClient }

func (c TailpipePluginClientWrapper) AddObserver() (proto.TailpipePlugin_AddObserverClient, error) {
	return c.client.AddObserver(context.Background(), &proto.Empty{})
}
func (c TailpipePluginClientWrapper) Collect(req *proto.CollectRequest) (*proto.CollectResponse, error) {
	return c.client.Collect(context.Background(), req)
}

func (c TailpipePluginClientWrapper) Describe() (*proto.DescribeResponse, error) {
	return c.client.Describe(context.Background(), &proto.DescribeRequest{})
}

func (c TailpipePluginClientWrapper) UpdateCollectionState(req *proto.UpdateCollectionStateRequest) (*proto.Empty, error) {
	return c.client.UpdateCollectionState(context.Background(), req)
}

func (c TailpipePluginClientWrapper) InitSource(req *proto.InitSourceRequest) (*proto.InitSourceResponse, error) {
	return c.client.InitSource(context.Background(), req)
}

func (c TailpipePluginClientWrapper) SaveCollectionState() (*proto.Empty, error) {
	return c.client.SaveCollectionState(context.Background(), &proto.Empty{})
}

func (c TailpipePluginClientWrapper) CloseSource() (*proto.Empty, error) {
	return c.client.CloseSource(context.Background(), &proto.Empty{})
}

func (c TailpipePluginClientWrapper) SourceCollect(req *proto.SourceCollectRequest) (*proto.Empty, error) {
	return c.client.SourceCollect(context.Background(), req)
}

// TailpipePluginServerWrapper is the gRPC server that TailpipePluginClient talks to.
type TailpipePluginServerWrapper struct {
	proto.UnimplementedTailpipePluginServer
	// This is the real implementation
	Impl TailpipePluginServer
}

func (s TailpipePluginServerWrapper) AddObserver(_ *proto.Empty, server proto.TailpipePlugin_AddObserverServer) error {
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
	return s.Impl.Describe(context.Background())
}

func (s TailpipePluginServerWrapper) UpdateCollectionState(_ context.Context, req *proto.UpdateCollectionStateRequest) (*proto.Empty, error) {
	return s.Impl.UpdateCollectionState(context.Background(), req)
}

func (s TailpipePluginServerWrapper) InitSource(_ context.Context, req *proto.InitSourceRequest) (*proto.InitSourceResponse, error) {
	return s.Impl.InitSource(context.Background(), req)
}

func (s TailpipePluginServerWrapper) SaveCollectionState(_ context.Context, req *proto.Empty) (*proto.Empty, error) {
	return s.Impl.SaveCollectionState(context.Background(), req)
}

func (s TailpipePluginServerWrapper) CloseSource(_ context.Context, req *proto.Empty) (*proto.Empty, error) {
	return s.Impl.CloseSource(context.Background(), req)
}

func (s TailpipePluginServerWrapper) SourceCollect(_ context.Context, req *proto.SourceCollectRequest) (*proto.Empty, error) {
	return s.Impl.SourceCollect(context.Background(), req)
}
