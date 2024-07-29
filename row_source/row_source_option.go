package row_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

// RowSourceOption is a function that can be used to configure a RowSource
// NOTE: individual options are specific to specific row source types
// RowSourceOption accepts the base Observable interface,
// and each option must implement a safe type assertion to the specific row source type
type RowSourceOption func(observable.Observable)

// WithMapper is used when creating an ArtifactRowSource
// It adds a mapper to the row source
func WithMapper(mappers ...artifact.Mapper) RowSourceOption {
	return func(r observable.Observable) {
		if a, ok := r.(MappedRowSource); ok {
			a.AddMappers(mappers...)
		}
	}
}

// WithLoader is used when creating an ArtifactRowSource
// It sets the a loader to the row source
func WithLoader(loader artifact.Loader) RowSourceOption {
	return func(r observable.Observable) {
		if a, ok := r.(MappedRowSource); ok {
			a.SetLoader(loader)
		}
	}
}

// WithRowPerLine is used when creating an ArtifactRowSource
// it specifies that the row source should treat each line as a separate row
func WithRowPerLine() RowSourceOption {
	return func(r observable.Observable) {
		if a, ok := r.(MappedRowSource); ok {
			a.SetRowPerLine(true)
		}
	}
}
