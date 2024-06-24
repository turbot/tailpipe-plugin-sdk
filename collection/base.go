package collection

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

// Base should be embedded in all tailpipe collection implementations
type Base struct {
	observable.Base
	// TODO validate this is set (or validate that data coming back is suitably enriched)
	EnrichFunc func(any, string) (any, error)
}

// Notify implements observable.Observer
// it handles all events which collections may receive
func (p *Base) Notify(event events.Event) error {
	switch e := event.(type) {
	case *events.Row:
		return p.HandleRowEvent(e.Row, e.Connection, e.Request)
	default:
		return fmt.Errorf("collection does not handle event type: %T", e)
	}
}

// HandleRowEvent is invoked when a Row event is received - enrich the row and publish it
func (p *Base) HandleRowEvent(row any, connection string, req *proto.CollectRequest) error {
	// if an enrich func is set, use it to enrich the row
	if p.EnrichFunc == nil {
		// error!
		return fmt.Errorf("no enrich function set")
	}
	enrichedRow, err := p.EnrichFunc(row, connection)
	if err != nil {
		return err
	}

	return p.NotifyObservers(events.NewRowEvent(req, connection, enrichedRow))
}
