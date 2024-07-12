package artifact

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type TransparentExtractorSink struct {
	ExtractorSinkBase
}

func NewTransparentExtractorSink() *TransparentExtractorSink {
	return &TransparentExtractorSink{}
}

// Notify implements observable.Observer
func (t *TransparentExtractorSink) Notify(event events.Event) error {
	switch e := event.(type) {
	case *events.ArtifactExtracted:
		return t.ExtractArtifactRows(context.Background(), e.Request, e.Artifact)
	default:
		return fmt.Errorf("CloudtrailExtractorSink received unexpected event type: %T", e)
	}
}

func (t *TransparentExtractorSink) ExtractArtifactRows(ctx context.Context, req *proto.CollectRequest, a *types.Artifact) error {
	// just send data as row
	return t.OnRow(req, a.Data, a.EnrichmentFields)
}
