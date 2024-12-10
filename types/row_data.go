package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
)

// RowData is a container for the data and metadata of an row
// It is used to pass data the [Loader]
// The RowData returned by the loader  is used as the payload of a [events.Row] which is sent to the [table.Table]
type RowData struct {
	Data             any
	SourceEnrichment *enrichment.SourceEnrichment
}

//// ArtifactDataOption is a function that can be used to set options on an RowData
//type ArtifactDataOption func(*RowData)
//
//// WithSourceEnrichment is an option that can be used to set the metadata on an RowData
//func WithSourceEnrichment(sourceEnrichment *SourceEnrichment) ArtifactDataOption {
//	return func(a *RowData) {
//		a.SourceEnrichment = sourceEnrichment
//	}
//}
//
//// NewData creates a new RowData, applying the specified [ArtifactDataOption]s
//func NewData(record any) *RowData {
//	return  &RowData{
//		Data: record,
//	}
//}
