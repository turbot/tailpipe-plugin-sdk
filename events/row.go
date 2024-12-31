package events

import (
	"encoding/json"

	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// NOTE: Row DOES NOT implement ToProto - we do not send row events over protobuf - the volume of data is too high

type Row struct {
	Base
	ExecutionId string

	// enrichment values passed from the source to the collection to include in the enrichment process
	SourceMetadata  schema.SourceEnrichment
	Row             any
	CollectionState json.RawMessage
}

type RowEventOption func(*Row)

func WithSourceEnrichment(sourceMetadata *schema.SourceEnrichment) RowEventOption {
	return func(r *Row) {
		if sourceMetadata != nil {
			r.SourceMetadata = *sourceMetadata
		}
	}
}
func NewRowEvent(executionId string, row any, paging json.RawMessage, opts ...RowEventOption) *Row {
	r := &Row{
		ExecutionId:     executionId,
		Row:             row,
		CollectionState: paging,
		SourceMetadata:  schema.SourceEnrichment{},
	}
	for _, opt := range opts {
		opt(r)
	}

	return r
}
