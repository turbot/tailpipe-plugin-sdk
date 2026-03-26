package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Event interface {
	isEvent()
}

// ProtoEvent is an interface for events that can be converted to a proto.Event
type ProtoEvent interface {
	ToProto() *proto.Event
}

// Base is the base struct for all events - it implements the marker function isEvent
type Base struct {
}

func (b *Base) isEvent() {}
