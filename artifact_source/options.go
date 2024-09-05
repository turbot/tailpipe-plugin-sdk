package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
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

// WithArtifactMapper is used when creating an ArtifactSourceBase
// It adds a mapper to the row source
func WithArtifactMapper(mappers ...artifact_mapper.Mapper) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.AddMappers(mappers...)
		}
		return nil
	}
}

// WithArtifactLoader is used when creating an ArtifactSourceBase
// It sets the a loader to the row source
func WithArtifactLoader(loader artifact_loader.Loader) row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetLoader(loader)
		}
		return nil
	}
}

// WithRowPerLine is used when creating an ArtifactSourceBase
// it specifies that the row source should treat each line as a separate row
func WithRowPerLine() row_source.RowSourceOption {
	return func(r row_source.RowSource) error {
		if a, ok := r.(ArtifactSource); ok {
			a.SetRowPerLine(true)
		}
		return nil
	}
}
