package row_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

type Base struct {
	observable.Base
}

func (a *Base) Close() error {
	return nil
}

// OnRow is called by the source when it has a row to send
func (a *Base) OnRow(ctx context.Context, row any, sourceEnrichmentFields *enrichment.CommonFields) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	return a.NotifyObservers(ctx, events.NewRowEvent(executionId, row, sourceEnrichmentFields))
}
