package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

// WithMapper is used when creating an ArtifactSourceBase
// It adds a mapper to the row source
func WithMapper(mappers ...artifact_mapper.Mapper) row_source.RowSourceOption {
	return func(r row_source.RowSource) {
		if a, ok := r.(ArtifactSource); ok {
			a.AddMappers(mappers...)
		}
	}
}

// WithLoader is used when creating an ArtifactSourceBase
// It sets the a loader to the row source
func WithLoader(loader artifact_loader.Loader) row_source.RowSourceOption {
	return func(r row_source.RowSource) {
		if a, ok := r.(ArtifactSource); ok {
			a.SetLoader(loader)
		}
	}
}

// WithRowPerLine is used when creating an ArtifactSourceBase
// it specifies that the row source should treat each line as a separate row
func WithRowPerLine() row_source.RowSourceOption {
	return func(r row_source.RowSource) {
		if a, ok := r.(ArtifactSource); ok {
			a.SetRowPerLine(true)
		}
	}
}
