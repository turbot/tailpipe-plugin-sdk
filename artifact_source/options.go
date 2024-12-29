package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

// WithDefaultArtifactSourceConfig sets the default config, e.g. file layout, IF it has not been set from config
func WithDefaultArtifactSourceConfig(config *artifact_source_config.ArtifactSourceConfigBase) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactRowSourceOptionHandler); ok {
			a.SetDefaultConfig(config)
		}
		return nil
	}
}

// WithArtifactLoader is used to specify an artifact loader
func WithArtifactLoader(loader artifact_loader.Loader) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactRowSourceOptionHandler); ok {
			a.SetLoader(loader)
		}
		return nil
	}
}

// WithArtifactExtractor is used to specify an artifact extractor
// this is needed if the artifact contains a collection of rows which needs explicit extraction
// (not this is in addition to the default extraction performed by the loaded)
func WithArtifactExtractor(extractor Extractor) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactRowSourceOptionHandler); ok {
			a.SetExtractor(extractor)
		}
		return nil
	}
}

// WithRowPerLine is used when creating an ArtifactSourceImpl
// it specifies that the row source should treat each line as a separate row
func WithRowPerLine() row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactRowSourceOptionHandler); ok {
			a.SetRowPerLine(true)
		}
		return nil
	}
}

// WithSkipHeaderRow is used when creating an ArtifactSourceImpl
// it specifies that the row source should skip the first row (header row).
func WithSkipHeaderRow() row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactRowSourceOptionHandler); ok {
			a.SetSkipHeaderRow(true)
		}
		return nil
	}
}
