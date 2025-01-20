package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// NOTE: Row DOES NOT implement ToProto - we do not send row events over protobuf - the volume of data is too high

type RowExtracted struct {
	Base
	ExecutionId string

	// enrichment values passed from the source to the collection to include in the enrichment process
	SourceEnrichment schema.SourceEnrichment
	Row              any
}

func NewRowExtractedEvent(executionId string, row any, SourceEnrichmens schema.SourceEnrichment) *RowExtracted {
	r := &RowExtracted{
		ExecutionId:      executionId,
		Row:              row,
		SourceEnrichment: SourceEnrichmens,
	}

	return r
}
