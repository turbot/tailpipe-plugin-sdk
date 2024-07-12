package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Row struct {
	Base
	Request *proto.CollectRequest

	// TODO maybe pass multiple rows?

	// enrichment values passed from the source to the collection to include in the enrichment process
	EnrichmentFields *enrichment.CommonFields
	Row              any
}

func NewRowEvent(request *proto.CollectRequest, row any, enrichmentFields *enrichment.CommonFields) *Row {
	return &Row{
		Request:          request,
		EnrichmentFields: enrichmentFields,
		Row:              row,
	}
}

//
//// ToProto converts the event to a proto.Event
//func (r *Row) ToProto() *proto.Event {
//	// there is no proto for a row event
//	// we should never call toProto for a Row event
//	panic("Row event should not be converted to proto")
//}
//
