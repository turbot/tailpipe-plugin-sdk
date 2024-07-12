package events

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type Completed struct {
	Base
	Request       *proto.CollectRequest
	RowCount      int
	ChunksWritten int
	Err           error
}

func NewCompletedEvent(request *proto.CollectRequest, rowCount int, chunksWritten int, err error) *Completed {
	return &Completed{
		Request:       request,
		RowCount:      rowCount,
		ChunksWritten: chunksWritten,
		Err:           err,
	}
}

func (c *Completed) ToProto() *proto.Event {
	return proto.NewCompleteEvent(c.Request.ExecutionId, c.RowCount, c.ChunksWritten, c.Err)
}
