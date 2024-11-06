package events

import (
	"encoding/json"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
)

type Row struct {
	Base
	ExecutionId string

	// enrichment values passed from the source to the collection to include in the enrichment process
	EnrichmentFields *enrichment.CommonFields
	Row              any
	CollectionState  json.RawMessage
}

type RowEventOption func(*Row)

func WithEnrichmentFields(enrichmentFields *enrichment.CommonFields) RowEventOption {
	return func(r *Row) {
		r.EnrichmentFields = enrichmentFields
	}
}
func NewRowEvent(executionId string, row any, paging json.RawMessage, opts ...RowEventOption) *Row {
	r := &Row{
		ExecutionId:      executionId,
		Row:              row,
		CollectionState:  paging,
		EnrichmentFields: &enrichment.CommonFields{},
	}
	for _, opt := range opts {
		opt(r)
	}

	return r
}
