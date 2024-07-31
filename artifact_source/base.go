package artifact_source

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// TODO #config
// base temp dir for all artifact sources
// this should be configurable in the plugin config?
const BaseTmpDir = "/tmp/tailpipe"

type Base struct {
	observable.Base
	// TODO #config should this be in base - means the risk that a derived struct will not set it
	TmpDir     string
	PagingData paging.Data
}

func (b *Base) Close() error {
	return nil
}

func (b *Base) OnArtifactDiscovered(ctx context.Context, info *types.ArtifactInfo) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	if err = b.NotifyObservers(ctx, events.NewArtifactDiscoveredEvent(executionId, info)); err != nil {
		return fmt.Errorf("error notifying observers of discovered artifact: %w", err)
	}
	return nil
}

// GetPagingDataSchema should be overriden by the RowSource implementation to return the paging data schema
// base implementation returns nil
func (b *Base) GetPagingDataSchema() paging.Data {
	return nil
}

func (b *Base) SetPagingData(data paging.Data) {
	b.PagingData = data
}

func (b *Base) OnArtifactDownloaded(ctx context.Context, info *types.ArtifactInfo, paging paging.Data) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	if err := b.NotifyObservers(ctx, events.NewArtifactDownloadedEvent(executionId, info, paging)); err != nil {
		return fmt.Errorf("error notifying observers of downloaded artifact: %w", err)
	}
	return nil
}

func (b *Base) Mapper() func() artifact_mapper.Mapper {
	return nil
}
