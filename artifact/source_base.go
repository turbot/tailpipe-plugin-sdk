package artifact

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type SourceBase struct {
	observable.Base
}

func (s *SourceBase) Close() error {
	return nil
}

// Mapper implenments the Source interface
// by default no source specific mapper is required
func (a *SourceBase) Mapper() func() Mapper {
	return nil
}

func (s *SourceBase) OnArtifactDiscovered(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo) error {
	err := s.NotifyObservers(ctx, events.NewArtifactDiscoveredEvent(req, info))

	if err != nil {
		return fmt.Errorf("error notifying observers of discovered artifact: %w", err)
	}
	return nil
}

func (s *SourceBase) OnArtifactDownloaded(ctx context.Context, req *proto.CollectRequest, info *types.ArtifactInfo) error {
	err := s.NotifyObservers(ctx, events.NewArtifactDownloadedEvent(req, info))

	if err != nil {
		return fmt.Errorf("error notifying observers of downloaded artifact: %w", err)
	}
	return nil
}
