package artifact

import (
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

func (s *SourceBase) OnArtifactDiscovered(req *proto.CollectRequest, info *types.ArtifactInfo) error {
	err := s.NotifyObservers(events.NewArtifactDiscoveredEvent(req, info))

	if err != nil {
		return fmt.Errorf("error notifying observers of discovered artifact: %w", err)
	}
	return nil
}

func (s *SourceBase) OnArtifactDownloaded(req *proto.CollectRequest, info *types.ArtifactInfo) error {
	err := s.NotifyObservers(events.NewArtifactDownloadedEvent(req, info))

	if err != nil {
		return fmt.Errorf("error notifying observers of downloaded artifact: %w", err)
	}
	return nil
}
