package artifact_row_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

// WithMapper is used when creating an Base
// It adds a mapper to the row source
func WithMapper(mappers ...artifact_mapper.Mapper) row_source.RowSourceOption {
	return func(r observable.Observable) {
		if a, ok := r.(*Base); ok {
			a.AddMappers(mappers...)
		}
	}
}

// WithLoader is used when creating an Base
// It sets the a loader to the row source
func WithLoader(loader artifact_loader.Loader) row_source.RowSourceOption {
	return func(r observable.Observable) {
		if a, ok := r.(*Base); ok {
			a.SetLoader(loader)
		}
	}
}

// WithRowPerLine is used when creating an Base
// it specifies that the row source should treat each line as a separate row
func WithRowPerLine() row_source.RowSourceOption {
	return func(r observable.Observable) {
		if a, ok := r.(*Base); ok {
			a.SetRowPerLine(true)
		}
	}
}
