package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Started struct {
	Base
	ExecutionId string
}

func NewStartedEvent(executionId string) *Started {
	return &Started{
		ExecutionId: executionId,
	}
}

func (s *Started) ToProto() *proto.Event {
	return &proto.Event{
		Event: &proto.Event_StartedEvent{
			StartedEvent: &proto.EventStarted{
				ExecutionId: s.ExecutionId,
			},
		},
	}
}
