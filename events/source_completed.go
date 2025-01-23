package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// SourceComplete is an event that is fired when a plugin used a source has completed a source collection
type SourceComplete struct {
	Base
	ExecutionId string
	Err         error
}

func NewSourceCompletedEvent(executionId string, err error) *SourceComplete {
	return &SourceComplete{
		ExecutionId: executionId,
		Err:         err,
	}
}

func (c *SourceComplete) ToProto() *proto.Event {
	errString := ""
	if c.Err != nil {
		errString = c.Err.Error()
	}

	return &proto.Event{
		Event: &proto.Event_SourceCompleteEvent{
			SourceCompleteEvent: &proto.EventSourceComplete{
				ExecutionId: c.ExecutionId,
				Error:       errString,
			},
		},
	}
}

func SourceCompleteFromProto(e *proto.Event) Event {
	return &SourceComplete{
		ExecutionId: e.GetCompleteEvent().ExecutionId,
		Err:         nil,
	}
}
