package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// NOTE: Row DOES NOT implement ToProto - we do not send row events over protobuf - the volume of data is too high

type Row struct {
	Base
	ExecutionId string

	// enrichment values passed from the source to the collection to include in the enrichment process
	SourceEnrichment schema.SourceEnrichment
	Row              any
}

type RowEventOption func(*Row)

func WithSourceEnrichment(sourceMetadata *schema.SourceEnrichment) RowEventOption {
	return func(r *Row) {
		if sourceMetadata != nil {
			r.SourceEnrichment = *sourceMetadata
		}
	}
}
func NewRowEvent(executionId string, row any, opts ...RowEventOption) *Row {
	r := &Row{
		ExecutionId:      executionId,
		Row:              row,
		SourceEnrichment: schema.SourceEnrichment{},
	}
	for _, opt := range opts {
		opt(r)
	}

	return r
}
