package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Error struct {
	Base
	Request *proto.CollectRequest
	Err     error
}

func NewErrorEvent(request *proto.CollectRequest, err error) *Error {
	return &Error{
		Request: request,
		Err:     err,
	}
}

func (c *Error) ToProto() *proto.Event {
	return proto.NewErrorEvent(c.Request.ExecutionId, c.Err)
}
