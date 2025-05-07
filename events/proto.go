package events

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// EventFromProto converts a proto.Event to an Event
// NOTE: this function is used for sources implemented in external plugins so handles source events ONLY
func EventFromProto(e *proto.Event) (Event, error) {
	switch e.Event.(type) {
	case *proto.Event_ArtifactDiscoveredEvent:
		return ArtifactDiscoveredFromProto(e), nil
	case *proto.Event_ArtifactExtractedEvent:
		return ArtifactExtractedFromProto(e), nil
	case *proto.Event_ArtifactDownloadedEvent:
		return ArtifactDownloadedFromProto(e), nil
	case *proto.Event_HeaderEvent:
		return HeaderFromProto(e), nil
	case *proto.Event_SourceCompleteEvent:
		return SourceCompleteFromProto(e), nil
	case *proto.Event_ErrorEvent:
		return ErrorFromProto(e), nil
	case *proto.Event_StartedEvent:
		return StartedFromProto(e), nil

	case *proto.Event_StatusEvent:
		return StatusFromProto(e), nil
	case *proto.Event_ChunkWrittenEvent:
		return ChunkWrittenFromProto(e), nil
	case *proto.Event_CompleteEvent:
		return CompleteFromProto(e), nil

	default:
		return nil, fmt.Errorf("event %s not expected from source", e)
	}
}
