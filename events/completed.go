package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Completed struct {
	Base
	ExecutionId   string
	RowCount      int
	ChunksWritten int
	Err           error
	Timing        types.TimingMap
}

func NewCompletedEvent(executionId string, rowCount int, chunksWritten int, timing types.TimingMap, err error) *Completed {
	return &Completed{
		ExecutionId:   executionId,
		RowCount:      rowCount,
		ChunksWritten: chunksWritten,
		Timing:        timing,
		Err:           err,
	}
}

func (c *Completed) ToProto() *proto.Event {
	return proto.NewCompleteEvent(c.ExecutionId, c.RowCount, c.ChunksWritten, c.Timing, c.Err)
}
