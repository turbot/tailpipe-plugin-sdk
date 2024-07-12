package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Started struct {
	Base
	Request *proto.CollectRequest
}

func NewStartedEvent(request *proto.CollectRequest) *Started {
	return &Started{
		Request: request,
	}
}

func (s *Started) ToProto() *proto.Event {
	return proto.NewStartedEvent(s.Request.ExecutionId)
}
