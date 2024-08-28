package types

import "github.com/turbot/tailpipe-plugin-sdk/enrichment"

type ArtifactInfoOpts func(*ArtifactInfo)

func WithEnrichmentFields(enrichmentFields *enrichment.CommonFields) ArtifactInfoOpts {
	return func(i *ArtifactInfo) {
		i.EnrichmentFields = enrichmentFields
	}
}
