package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

type ArtifactToJsonConverterImpl[S parse.Config] struct {
}

func (c *ArtifactToJsonConverterImpl[S]) GetSourceMetadata(_ S) []*SourceMetadata[*DynamicRow] {
	return []*SourceMetadata[*DynamicRow]{
		{
			SourceName: constants.ArtifactSourceIdentifier,
			// set a null loader so we don't receive row events - instead we implement ArtifactToJsonConverter
			// to convert the artifact to JSONL directly
			Options: []row_source.RowSourceOption{artifact_source.WithArtifactLoader(artifact_loader.NewNullLoader())},
		},
	}
}

func (c *ArtifactToJsonConverterImpl[S]) EnrichRow(_ *DynamicRow, _ S, _ enrichment.SourceEnrichment) (*DynamicRow, error) {
	// this should never be called as we are using a null loader which means we will not receive row events
	panic("EnrichRow should never be called for tables implementing ArtifactToJsonConverter")
}

func (c *ArtifactToJsonConverterImpl[S]) GetArtifactConversionQuery(_, _ string, _ S) (string, error) {
	panic("GetArtifactConversionQuery must be implemented by struct embedding ArtifactToJsonConverterImpl")
}
