package row_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
)

type Base struct {
	observable.Base
}

func (a *Base) Close() error {
	return nil
}

// OnRow is called by the source when it has a row to send
func (a *Base) OnRow(ctx context.Context, row *artifact.ArtifactData, pagingData paging.Data) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	return a.NotifyObservers(ctx, events.NewRowEvent(executionId, row.Data, pagingData, events.WithEnrichmentFields(row.Metadata)))
}
