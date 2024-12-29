package plugin

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// ObserverWrapper mapd between proto Observer and the plugin Observer
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
