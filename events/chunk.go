package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Chunk struct {
	Base
	ExecutionId string
	ChunkNumber int
}

func NewChunkEvent(executionId string, chunkNumber int) *Chunk {
	return &Chunk{
		ExecutionId: executionId,
		ChunkNumber: chunkNumber,
	}
}

// ToProto converts the event to a proto.Event
func (r *Chunk) ToProto() *proto.Event {
	return proto.NewChunkWrittenEvent(r.ExecutionId, r.ChunkNumber)
}
