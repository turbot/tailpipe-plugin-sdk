package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/paging"
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

type ArtifactDownloaded struct {
	Base
	ExecutionId string
	Info        *types.ArtifactInfo
	PagingData  paging.Data
}

func NewArtifactDownloadedEvent(executionId string, info *types.ArtifactInfo, paging paging.Data) *ArtifactDownloaded {
	return &ArtifactDownloaded{
		ExecutionId: executionId,
		Info:        info,
		PagingData:  paging,
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
