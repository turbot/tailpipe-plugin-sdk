package source

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

// Base should be embedded in all tailpipe collection implementations
type Base struct {
	observable.Base
}

// OnRow is called by the source when it has a row to send
func (p *Base) OnRow(req *proto.CollectRequest, row any, sourceEnrichmentFields *enrichment.CommonFields) error {
	return p.NotifyObservers(events.NewRowEvent(req, row, sourceEnrichmentFields))
}
