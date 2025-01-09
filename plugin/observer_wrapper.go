package plugin

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// ObserverWrapper mapd between proto Observer and the plugin Observer
type ObserverWrapper struct {
	protoObserver proto.TailpipePlugin_AddObserverServer
}

func NewObserverWrapper(protoObserver proto.TailpipePlugin_AddObserverServer) ObserverWrapper {
	return ObserverWrapper{protoObserver: protoObserver}
}

// Notify implements the Observer interface but sends to a proto stream
func (o ObserverWrapper) Notify(_ context.Context, e events.Event) error {
	if p, ok := e.(events.ProtoEvent); ok {
		return o.protoObserver.Send(p.ToProto())
	}
	// this event does not implement ProtoEvent, so do not send over protobuf
	return nil
}
