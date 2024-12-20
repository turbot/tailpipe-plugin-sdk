package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type ArtifactInfoOpts func(*ArtifactInfo)

func WithSourceEnrichment(sourceEnrichment *schema.SourceEnrichment) ArtifactInfoOpts {
	return func(i *ArtifactInfo) {
		i.SourceEnrichment = sourceEnrichment
	}
}
