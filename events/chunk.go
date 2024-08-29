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
	return proto.NewChunkWrittenEvent(r.ExecutionId, r.ChunkNumber, r.CollectionState)
}
