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
		pagingJson, err := getPagingJSON(paging)
		if err != nil {
			return nil, err
		}
		c.PagingData = pagingJson
	}

	return c, nil
}

func getPagingJSON(paging paging.Data) ([]byte, error) {
	// NOTE: lock the paging data to ensure it is not modified while we are serialising it
	mut := paging.GetMut()
	mut.RLock()
	defer mut.RUnlock()

	return json.Marshal(paging)
}

// ToProto converts the event to a proto.Event
func (r *Chunk) ToProto() *proto.Event {
	return proto.NewChunkWrittenEvent(r.ExecutionId, r.ChunkNumber, r.PagingData)
}
