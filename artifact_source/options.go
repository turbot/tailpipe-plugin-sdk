package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

// WithDefaultArtifactSourceConfig sets the default config, e.g. file layout, IF it has not been set from config
func WithDefaultArtifactSourceConfig(config *artifact_source_config.ArtifactSourceConfigBase) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetDefaultConfig(config)
		}
		return nil
	}
}

// WithArtifactExtractor is used to specify an artifact extractor
// this is needed if the artifact contains a collection of rows which needs explicit extraction
func WithArtifactExtractor(extractor Extractor) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetExtractor(extractor)
		}
		return nil
	}
}

// WithArtifactLoader is used when creating an ArtifactSourceImpl
// It sets the a loader to the row source
func WithArtifactLoader(loader artifact_loader.Loader) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetLoader(loader)
		}
		return nil
	}
}

// WithRowPerLine is used when creating an ArtifactSourceImpl
// it specifies that the row source should treat each line as a separate row
func WithRowPerLine() row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetRowPerLine(true)
		}
		return nil
	}
}

// WithSkipHeaderRow is used when creating an ArtifactSourceImpl
// it specifies that the row source should skip the first row (header row).
func WithSkipHeaderRow() row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetSkipHeaderRow(true)
		}
		return nil
	}
}
