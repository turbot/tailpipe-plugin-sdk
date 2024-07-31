package artifact_mapper

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Mapper is an interface which provides a method for mapping artifact data to a different format
// an [row_source.Base] may be configured to have one or more Mappers.
// Mappers provided by the SDK: [CloudwatchMapper]
type Mapper interface {
	Identifier() string
	// Map converts artifact data to a different format and either return it as rows,
	// or pass it on to the next mapper in the chain
	Map(context.Context, *types.RowData) ([]*types.RowData, error)
}
