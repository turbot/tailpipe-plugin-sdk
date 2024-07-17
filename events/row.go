package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
)

type Row struct {
	Base
	ExecutionId string

	// TODO maybe pass multiple rows?

	// enrichment values passed from the source to the collection to include in the enrichment process
	EnrichmentFields *enrichment.CommonFields
	Row              any
}

func NewRowEvent(executionId string, row any, enrichmentFields *enrichment.CommonFields) *Row {
	return &Row{
		ExecutionId:      executionId,
		EnrichmentFields: enrichmentFields,
		Row:              row,
	}
}
