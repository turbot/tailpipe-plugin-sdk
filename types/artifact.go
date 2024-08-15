package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
)

type ArtifactInfo struct {
	// if the artifact is has been dowloaded, Name will be the path to the downloaded file
	// and OriginalName will be the source path
	Name             string
	OriginalName     string
	EnrichmentFields *enrichment.CommonFields
	// TODO #observeability - add timings
}

// TODO add WithEnrichment options and update usage to call this instaed of just creating
func NewArtifactInfo(name string) *ArtifactInfo {
	return &ArtifactInfo{
		Name:         name,
		OriginalName: name,
	}
}
