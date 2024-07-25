package artifact

import (
	"context"
	"fmt"

	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type SourceBase struct {
	observable.Base
	tmpDir string
}

func (s *SourceBase) Close() error {
	return nil
}

// Mapper implenments the Source interface
// by default no source specific mapper is required
func (s *SourceBase) Mapper() func() Mapper {
	return nil
}

func (s *SourceBase) OnArtifactDiscovered(ctx context.Context, info *types.ArtifactInfo) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	if err = s.NotifyObservers(ctx, events.NewArtifactDiscoveredEvent(executionId, info)); err != nil {
		return fmt.Errorf("error notifying observers of discovered artifact: %w", err)
	}
	return nil
}

func (s *SourceBase) OnArtifactDownloaded(ctx context.Context, info *types.ArtifactInfo, paging paging.Data) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	if err := s.NotifyObservers(ctx, events.NewArtifactDownloadedEvent(executionId, info, paging)); err != nil {
		return fmt.Errorf("error notifying observers of downloaded artifact: %w", err)
	}
	return nil
}
