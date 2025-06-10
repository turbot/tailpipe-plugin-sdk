package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

type SourceMetadata[R any] struct {
	SourceName string

	Mapper  mappers.Mapper[R]
	Options []row_source.RowSourceOption
}
