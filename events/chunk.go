package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Chunk struct {
	Base
	Request     *proto.CollectRequest
	ChunkNumber int
}

func NewChunkEvent(request *proto.CollectRequest, chunkNumber int) *Chunk {
	return &Chunk{
		Request:     request,
		ChunkNumber: chunkNumber,
	}
}

// ToProto converts the event to a proto.Event
func (r *Chunk) ToProto() *proto.Event {
	return proto.NewChunkWrittenEvent(r.Request.ExecutionId, r.ChunkNumber)
}
