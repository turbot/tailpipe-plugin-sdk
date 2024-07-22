package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Completed struct {
	Base
	ExecutionId   string
	RowCount      int
	ChunksWritten int
	Err           error
}

func NewCompletedEvent(executionId string, rowCount int, chunksWritten int, err error) *Completed {
	return &Completed{
		ExecutionId:   executionId,
		RowCount:      rowCount,
		ChunksWritten: chunksWritten,
		Err:           err,
	}
}

func (c *Completed) ToProto() *proto.Event {
	return proto.NewCompleteEvent(c.ExecutionId, c.RowCount, c.ChunksWritten, c.Err)
}
