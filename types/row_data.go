package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// RowData is a container for the data and metadata of an row
// It is used to pass data the [Loader]
// The RowData returned by the loader  is used as the payload of a [events.Row] which is sent to the [table.Table]
type RowData struct {
	Data             any
	SourceEnrichment *schema.SourceEnrichment
}
