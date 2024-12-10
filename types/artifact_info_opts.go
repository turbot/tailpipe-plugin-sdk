package types

import "github.com/turbot/tailpipe-plugin-sdk/enrichment"

type ArtifactInfoOpts func(*ArtifactInfo)

func WithSourceEnrichment(sourceEnrichment *enrichment.SourceEnrichment) ArtifactInfoOpts {
	return func(i *ArtifactInfo) {
		i.SourceEnrichment = sourceEnrichment
	}
}
