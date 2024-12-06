package events

import (
	"encoding/json"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
)

type Row struct {
	Base
	ExecutionId string

	// enrichment values passed from the source to the collection to include in the enrichment process
	SourceMetadata  enrichment.SourceEnrichment
	Row             any
	CollectionState json.RawMessage
}

type RowEventOption func(*Row)

func WithSourceMetadata(sourceMetadata *enrichment.SourceEnrichment) RowEventOption {
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
		SourceMetadata:  enrichment.SourceEnrichment{},
	}
	for _, opt := range opts {
		opt(r)
	}

	return r
}
