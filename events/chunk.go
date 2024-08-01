package events

import (
	"encoding/json"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
)

type Chunk struct {
	Base
	ExecutionId string
	ChunkNumber int
	PagingData  json.RawMessage
}

func NewChunkEvent(executionId string, chunkNumber int, paging paging.Data) (*Chunk, error) {
	c := &Chunk{
		ExecutionId: executionId,
		ChunkNumber: chunkNumber,
	}

	// serialise the paging data to json
	if paging != nil {
		pagingJson, err := json.Marshal(paging)
		if err != nil {
			return nil, err
		}
		c.PagingData = pagingJson
	}

	return c, nil
}

// ToProto converts the event to a proto.Event
func (r *Chunk) ToProto() *proto.Event {
	return proto.NewChunkWrittenEvent(r.ExecutionId, r.ChunkNumber, r.PagingData)
}
