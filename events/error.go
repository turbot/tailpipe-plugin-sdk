package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Error struct {
	Base
	ExecutionId string
	Err         error
}

func NewErrorEvent(executionId string, err error) *Error {
	return &Error{
		ExecutionId: executionId,
		Err:         err,
	}
}

func (c *Error) ToProto() *proto.Event {
	return proto.NewErrorEvent(c.ExecutionId, c.Err)
}
