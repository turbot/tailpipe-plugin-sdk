package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type SourceMetadata[R types.RowStruct] struct {
	SourceName string
	MapperFunc func() Mapper[R]
	Options    []row_source.RowSourceOption
}
