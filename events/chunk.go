package events

import (
	"encoding/json"

	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Chunk struct {
	Base
	ExecutionId     string
	ChunkNumber     int
	CollectionState json.RawMessage
}

func NewChunkEvent(executionId string, chunkNumber int, collectionState json.RawMessage) *Chunk {
	return &Chunk{
		ExecutionId:     executionId,
		ChunkNumber:     chunkNumber,
		CollectionState: collectionState,
	}
}

// ToProto converts the event to a proto.Event
func (r *Chunk) ToProto() *proto.Event {
	return &proto.Event{
		Event: &proto.Event_ChunkWrittenEvent{
			ChunkWrittenEvent: &proto.EventChunkWritten{
				ExecutionId:     r.ExecutionId,
				ChunkNumber:     int32(r.ChunkNumber), //nolint:gosec // TODO look at integer overflow conversion
				CollectionState: r.CollectionState,
			},
		},
	}
}
