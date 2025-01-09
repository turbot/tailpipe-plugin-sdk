package events

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// SourceEventFromProto converts a proto.Event to an Event
// NOTE: this function is used for sources implemented in external plugins so handles source events ONLY
func SourceEventFromProto(e *proto.Event) (Event, error) {
	switch e.Event.(type) {
	case *proto.Event_ArtifactDiscoveredEvent:
		return ArtifactDiscoveredFromProto(e), nil
	case *proto.Event_ArtifactExtractedEvent:
		return ArtifactExtractedFromProto(e), nil
	case *proto.Event_ArtifactDownloadedEvent:
		return ArtifactDownloadedFromProto(e), nil
	case *proto.Event_SourceCompleteEvent:
		return SourceCompleteFromProto(e), nil
	default:
		return nil, fmt.Errorf("event %s not expected from source", e)
	}
}
