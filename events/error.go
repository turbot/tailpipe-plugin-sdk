package events

import (
	"errors"

	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

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
	return &proto.Event{
		Event: &proto.Event_ErrorEvent{
			ErrorEvent: &proto.EventError{
				ExecutionId: c.ExecutionId,
				Error:       c.Err.Error(),
			},
		},
	}

}

func ErrorFromProto(e *proto.Event) Event {
	return &Error{
		ExecutionId: e.GetErrorEvent().ExecutionId,
		Err:         errors.New(e.GetErrorEvent().GetError()),
	}
}
