package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// TODO kai add event base with marker function - remove ToProto from interface and derive a new ProtoEvent interface

type ArtifactDiscovered struct {
	Base
	Request *proto.CollectRequest
	Info    *types.ArtifactInfo
}

func NewArtifactDiscoveredEvent(request *proto.CollectRequest, info *types.ArtifactInfo) *ArtifactDiscovered {
	return &ArtifactDiscovered{
		Request: request,
		Info:    info,
	}
}

type ArtifactDownloaded struct {
	Base
	Request *proto.CollectRequest
	Info    *types.ArtifactInfo
}

func NewArtifactDownloadedEvent(request *proto.CollectRequest, info *types.ArtifactInfo) *ArtifactDownloaded {
	return &ArtifactDownloaded{
		Request: request,
		Info:    info,
	}
}

// ArtifactExtracted is an event that is fired by an ExtractorSource when it has extracted an artifact
// (but not yet processed it into rows)
type ArtifactExtracted struct {
	Base
	Request  *proto.CollectRequest
	Artifact *types.Artifact
}

func NewArtifactExtractedEvent(request *proto.CollectRequest, a *types.Artifact) *ArtifactExtracted {
	return &ArtifactExtracted{
		Request:  request,
		Artifact: a,
	}
}
