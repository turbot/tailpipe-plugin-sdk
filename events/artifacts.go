package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type ArtifactDiscovered struct {
	Base
	ExecutionId string
	Info        *types.ArtifactInfo
}

func NewArtifactDiscoveredEvent(executionId string, info *types.ArtifactInfo) *ArtifactDiscovered {
	return &ArtifactDiscovered{
		ExecutionId: executionId,
		Info:        info,
	}
}

func (c *ArtifactDiscovered) ToProto() *proto.Event {
	return &proto.Event{
		Event: &proto.Event_ArtifactDiscoveredEvent{
			ArtifactDiscoveredEvent: &proto.EventArtifactDiscovered{
				ExecutionId:  c.ExecutionId,
				ArtifactInfo: c.Info.ToProto(),
			},
		},
	}
}

func ArtifactDiscoveredFromProto(e *proto.Event) Event {
	return &ArtifactDiscovered{
		ExecutionId: e.GetArtifactDiscoveredEvent().ExecutionId,
		Info:        types.ArtifactInfoFromProto(e.GetArtifactDiscoveredEvent().ArtifactInfo),
	}
}

type ArtifactDownloaded struct {
	Base
	ExecutionId string
	Info        *types.ArtifactInfo
}

func NewArtifactDownloadedEvent(executionId string, info *types.ArtifactInfo) *ArtifactDownloaded {
	return &ArtifactDownloaded{
		ExecutionId: executionId,
		Info:        info,
	}
}

func (c *ArtifactDownloaded) ToProto() *proto.Event {
	return &proto.Event{
		Event: &proto.Event_ArtifactDownloadedEvent{
			ArtifactDownloadedEvent: &proto.EventArtifactDownloaded{
				ExecutionId:  c.ExecutionId,
				ArtifactInfo: c.Info.ToProto(),
			},
		},
	}
}

func ArtifactDownloadedFromProto(e *proto.Event) Event {
	return &ArtifactDownloaded{
		ExecutionId: e.GetArtifactDownloadedEvent().ExecutionId,
		Info:        types.ArtifactInfoFromProto(e.GetArtifactDownloadedEvent().ArtifactInfo),
	}
}

// ArtifactExtracted is an event that is fired by an ExtractorSource when it has extracted an artifact
// (but not yet processed it into rows)
type ArtifactExtracted struct {
	Base
	ExecutionId string
	Info        *types.ArtifactInfo
}

func NewArtifactExtractedEvent(executionId string, info *types.ArtifactInfo) *ArtifactExtracted {
	return &ArtifactExtracted{
		ExecutionId: executionId,
		Info:        info,
	}
}

// ToProto converts the event to a proto event
func (c *ArtifactExtracted) ToProto() *proto.Event {
	return &proto.Event{
		Event: &proto.Event_ArtifactExtractedEvent{
			ArtifactExtractedEvent: &proto.EventArtifactExtracted{
				ExecutionId:  c.ExecutionId,
				ArtifactInfo: c.Info.ToProto(),
			},
		},
	}
}

func ArtifactExtractedFromProto(e *proto.Event) Event {
	return &ArtifactExtracted{
		ExecutionId: e.GetArtifactExtractedEvent().ExecutionId,
		Info:        types.ArtifactInfoFromProto(e.GetArtifactExtractedEvent().ArtifactInfo),
	}
}
