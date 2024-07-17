package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// TODO kai add event base with marker function - remove ToProto from interface and derive a new ProtoEvent interface

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

// ArtifactExtracted is an event that is fired by an ExtractorSource when it has extracted an artifact
// (but not yet processed it into rows)
type ArtifactExtracted struct {
	Base
	ExecutionId string
	Artifact    *types.Artifact
}

func NewArtifactExtractedEvent(executionId string, a *types.Artifact) *ArtifactExtracted {
	return &ArtifactExtracted{
		ExecutionId: executionId,
		Artifact:    a,
	}
}
