package events

import (
	"encoding/json"
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
	PagingData  json.RawMessage
}

func NewArtifactDownloadedEvent(executionId string, info *types.ArtifactInfo, paging json.RawMessage) *ArtifactDownloaded {
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
	Info        *types.ArtifactInfo
}

func NewArtifactExtractedEvent(executionId string, info *types.ArtifactInfo) *ArtifactExtracted {
	return &ArtifactExtracted{
		ExecutionId: executionId,
		Info:        info,
	}
}
