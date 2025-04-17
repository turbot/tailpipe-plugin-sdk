package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Header struct {
	Base
	ExecutionId string
	Info        *types.ArtifactInfo
	Header      []string
}

func NewHeaderEvent(executionId string, info *types.ArtifactInfo, header []string) *Header {
	return &Header{
		ExecutionId: executionId,
		Info:        info,
		Header:      header,
	}
}

func (c *Header) ToProto() *proto.Event {
	return &proto.Event{
		Event: &proto.Event_HeaderEvent{
			HeaderEvent: &proto.EventHeader{
				ExecutionId:  c.ExecutionId,
				ArtifactInfo: c.Info.ToProto(),
				Header:       c.Header,
			},
		},
	}
}
