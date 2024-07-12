package row_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

type Base struct {
	observable.Base
}

func (a *Base) Close() error {
	return nil
}

// OnRow is called by the source when it has a row to send
func (a *Base) OnRow(req *proto.CollectRequest, row any, sourceEnrichmentFields *enrichment.CommonFields) error {
	return a.NotifyObservers(events.NewRowEvent(req, row, sourceEnrichmentFields))
}
