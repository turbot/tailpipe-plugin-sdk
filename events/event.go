package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Event interface {
	IsEvent()
}
type ProtoEvent interface {
	ToProto() *proto.Event
}

type Base struct {
}

func (b *Base) IsEvent() {}
