// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v5.29.2
// source: plugin.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	TailpipePlugin_Describe_FullMethodName              = "/proto.TailpipePlugin/Describe"
	TailpipePlugin_AddObserver_FullMethodName           = "/proto.TailpipePlugin/AddObserver"
	TailpipePlugin_Collect_FullMethodName               = "/proto.TailpipePlugin/Collect"
	TailpipePlugin_InitSource_FullMethodName            = "/proto.TailpipePlugin/InitSource"
	TailpipePlugin_UpdateCollectionState_FullMethodName = "/proto.TailpipePlugin/UpdateCollectionState"
	TailpipePlugin_CloseSource_FullMethodName           = "/proto.TailpipePlugin/CloseSource"
	TailpipePlugin_SaveCollectionState_FullMethodName   = "/proto.TailpipePlugin/SaveCollectionState"
	TailpipePlugin_SourceCollect_FullMethodName         = "/proto.TailpipePlugin/SourceCollect"
)

// TailpipePluginClient is the client API for TailpipePlugin service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TailpipePluginClient interface {
	Describe(ctx context.Context, in *DescribeRequest, opts ...grpc.CallOption) (*DescribeResponse, error)
	AddObserver(ctx context.Context, in *Empty, opts ...grpc.CallOption) (TailpipePlugin_AddObserverClient, error)
	Collect(ctx context.Context, in *CollectRequest, opts ...grpc.CallOption) (*CollectResponse, error)
	InitSource(ctx context.Context, in *InitSourceRequest, opts ...grpc.CallOption) (*InitSourceResponse, error)
	UpdateCollectionState(ctx context.Context, in *UpdateCollectionStateRequest, opts ...grpc.CallOption) (*Empty, error)
	CloseSource(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*Empty, error)
	SaveCollectionState(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*Empty, error)
	SourceCollect(ctx context.Context, in *SourceCollectRequest, opts ...grpc.CallOption) (*Empty, error)
}

type tailpipePluginClient struct {
	cc grpc.ClientConnInterface
}

func NewTailpipePluginClient(cc grpc.ClientConnInterface) TailpipePluginClient {
	return &tailpipePluginClient{cc}
}

func (c *tailpipePluginClient) Describe(ctx context.Context, in *DescribeRequest, opts ...grpc.CallOption) (*DescribeResponse, error) {
	out := new(DescribeResponse)
	err := c.cc.Invoke(ctx, TailpipePlugin_Describe_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *tailpipePluginClient) AddObserver(ctx context.Context, in *Empty, opts ...grpc.CallOption) (TailpipePlugin_AddObserverClient, error) {
	stream, err := c.cc.NewStream(ctx, &TailpipePlugin_ServiceDesc.Streams[0], TailpipePlugin_AddObserver_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &tailpipePluginAddObserverClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type TailpipePlugin_AddObserverClient interface {
	Recv() (*Event, error)
	grpc.ClientStream
}

type tailpipePluginAddObserverClient struct {
	grpc.ClientStream
}

func (x *tailpipePluginAddObserverClient) Recv() (*Event, error) {
	m := new(Event)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *tailpipePluginClient) Collect(ctx context.Context, in *CollectRequest, opts ...grpc.CallOption) (*CollectResponse, error) {
	out := new(CollectResponse)
	err := c.cc.Invoke(ctx, TailpipePlugin_Collect_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *tailpipePluginClient) InitSource(ctx context.Context, in *InitSourceRequest, opts ...grpc.CallOption) (*InitSourceResponse, error) {
	out := new(InitSourceResponse)
	err := c.cc.Invoke(ctx, TailpipePlugin_InitSource_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *tailpipePluginClient) UpdateCollectionState(ctx context.Context, in *UpdateCollectionStateRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, TailpipePlugin_UpdateCollectionState_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *tailpipePluginClient) CloseSource(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, TailpipePlugin_CloseSource_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *tailpipePluginClient) SaveCollectionState(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, TailpipePlugin_SaveCollectionState_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *tailpipePluginClient) SourceCollect(ctx context.Context, in *SourceCollectRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, TailpipePlugin_SourceCollect_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TailpipePluginServer is the server API for TailpipePlugin service.
// All implementations must embed UnimplementedTailpipePluginServer
// for forward compatibility
type TailpipePluginServer interface {
	Describe(context.Context, *DescribeRequest) (*DescribeResponse, error)
	AddObserver(*Empty, TailpipePlugin_AddObserverServer) error
	Collect(context.Context, *CollectRequest) (*CollectResponse, error)
	InitSource(context.Context, *InitSourceRequest) (*InitSourceResponse, error)
	UpdateCollectionState(context.Context, *UpdateCollectionStateRequest) (*Empty, error)
	CloseSource(context.Context, *Empty) (*Empty, error)
	SaveCollectionState(context.Context, *Empty) (*Empty, error)
	SourceCollect(context.Context, *SourceCollectRequest) (*Empty, error)
	mustEmbedUnimplementedTailpipePluginServer()
}

// UnimplementedTailpipePluginServer must be embedded to have forward compatible implementations.
type UnimplementedTailpipePluginServer struct {
}

func (UnimplementedTailpipePluginServer) Describe(context.Context, *DescribeRequest) (*DescribeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Describe not implemented")
}
func (UnimplementedTailpipePluginServer) AddObserver(*Empty, TailpipePlugin_AddObserverServer) error {
	return status.Errorf(codes.Unimplemented, "method AddObserver not implemented")
}
func (UnimplementedTailpipePluginServer) Collect(context.Context, *CollectRequest) (*CollectResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Collect not implemented")
}
func (UnimplementedTailpipePluginServer) InitSource(context.Context, *InitSourceRequest) (*InitSourceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InitSource not implemented")
}
func (UnimplementedTailpipePluginServer) UpdateCollectionState(context.Context, *UpdateCollectionStateRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateCollectionState not implemented")
}
func (UnimplementedTailpipePluginServer) CloseSource(context.Context, *Empty) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CloseSource not implemented")
}
func (UnimplementedTailpipePluginServer) SaveCollectionState(context.Context, *Empty) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SaveCollectionState not implemented")
}
func (UnimplementedTailpipePluginServer) SourceCollect(context.Context, *SourceCollectRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SourceCollect not implemented")
}
func (UnimplementedTailpipePluginServer) mustEmbedUnimplementedTailpipePluginServer() {}

// UnsafeTailpipePluginServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TailpipePluginServer will
// result in compilation errors.
type UnsafeTailpipePluginServer interface {
	mustEmbedUnimplementedTailpipePluginServer()
}

func RegisterTailpipePluginServer(s grpc.ServiceRegistrar, srv TailpipePluginServer) {
	s.RegisterService(&TailpipePlugin_ServiceDesc, srv)
}

func _TailpipePlugin_Describe_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DescribeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TailpipePluginServer).Describe(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TailpipePlugin_Describe_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TailpipePluginServer).Describe(ctx, req.(*DescribeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TailpipePlugin_AddObserver_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(TailpipePluginServer).AddObserver(m, &tailpipePluginAddObserverServer{stream})
}

type TailpipePlugin_AddObserverServer interface {
	Send(*Event) error
	grpc.ServerStream
}

type tailpipePluginAddObserverServer struct {
	grpc.ServerStream
}

func (x *tailpipePluginAddObserverServer) Send(m *Event) error {
	return x.ServerStream.SendMsg(m)
}

func _TailpipePlugin_Collect_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CollectRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TailpipePluginServer).Collect(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TailpipePlugin_Collect_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TailpipePluginServer).Collect(ctx, req.(*CollectRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TailpipePlugin_InitSource_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InitSourceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TailpipePluginServer).InitSource(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TailpipePlugin_InitSource_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TailpipePluginServer).InitSource(ctx, req.(*InitSourceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TailpipePlugin_UpdateCollectionState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateCollectionStateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TailpipePluginServer).UpdateCollectionState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TailpipePlugin_UpdateCollectionState_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TailpipePluginServer).UpdateCollectionState(ctx, req.(*UpdateCollectionStateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TailpipePlugin_CloseSource_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TailpipePluginServer).CloseSource(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TailpipePlugin_CloseSource_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TailpipePluginServer).CloseSource(ctx, req.(*Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _TailpipePlugin_SaveCollectionState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TailpipePluginServer).SaveCollectionState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TailpipePlugin_SaveCollectionState_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TailpipePluginServer).SaveCollectionState(ctx, req.(*Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _TailpipePlugin_SourceCollect_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SourceCollectRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TailpipePluginServer).SourceCollect(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TailpipePlugin_SourceCollect_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TailpipePluginServer).SourceCollect(ctx, req.(*SourceCollectRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// TailpipePlugin_ServiceDesc is the grpc.ServiceDesc for TailpipePlugin service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var TailpipePlugin_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.TailpipePlugin",
	HandlerType: (*TailpipePluginServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Describe",
			Handler:    _TailpipePlugin_Describe_Handler,
		},
		{
			MethodName: "Collect",
			Handler:    _TailpipePlugin_Collect_Handler,
		},
		{
			MethodName: "InitSource",
			Handler:    _TailpipePlugin_InitSource_Handler,
		},
		{
			MethodName: "UpdateCollectionState",
			Handler:    _TailpipePlugin_UpdateCollectionState_Handler,
		},
		{
			MethodName: "CloseSource",
			Handler:    _TailpipePlugin_CloseSource_Handler,
		},
		{
			MethodName: "SaveCollectionState",
			Handler:    _TailpipePlugin_SaveCollectionState_Handler,
		},
		{
			MethodName: "SourceCollect",
			Handler:    _TailpipePlugin_SourceCollect_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "AddObserver",
			Handler:       _TailpipePlugin_AddObserver_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "plugin.proto",
}
