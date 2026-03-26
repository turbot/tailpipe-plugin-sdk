package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// DownloadedArtifactInfo contains information about a downloaded artifact
// is the same as ArtifactInfo, but with a size field
type DownloadedArtifactInfo struct {
	ArtifactInfo
	// the path of the downloaded artifact
	LocalName string `json:"local_name"`

	Size int64 `json:"size"`
}

func NewDownloadedArtifactInfo(artifactInfo *ArtifactInfo, localName string, size int64) *DownloadedArtifactInfo {
	res := &DownloadedArtifactInfo{
		ArtifactInfo: *artifactInfo,
		LocalName:    localName,
		Size:         size,
	}

	return res
}

func DownloadedArtifactInfoFromProto(info *proto.DownloadedArtifactInfo) *DownloadedArtifactInfo {
	enrichment := schema.SourceEnrichmentFromProto(info.SourceEnrichment)

	return &DownloadedArtifactInfo{
		ArtifactInfo: ArtifactInfo{
			Name:             info.OriginalName,
			SourceEnrichment: enrichment,
		},
		LocalName: info.LocalName,
		Size:      info.Size,
	}
}

func (a *DownloadedArtifactInfo) ToProto() *proto.DownloadedArtifactInfo {
	return &proto.DownloadedArtifactInfo{
		LocalName:        a.LocalName,
		OriginalName:     a.Name,
		SourceEnrichment: a.SourceEnrichment.ToProto(),
		Size:             a.Size,
	}
}
