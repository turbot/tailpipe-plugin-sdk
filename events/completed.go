package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Complete struct {
	Base
	ExecutionId   string
	RowCount      int
	ChunksWritten int
	Err           error
}

func NewCompletedEvent(executionId string, rowCount int, chunksWritten int, err error) *Complete {
	return &Complete{
		ExecutionId:   executionId,
		RowCount:      rowCount,
		ChunksWritten: chunksWritten,
		Err:           err,
	}
}

func (c *Complete) ToProto() *proto.Event {
	errString := ""
	if c.Err != nil {
		errString = c.Err.Error()
	}

	return &proto.Event{
		Event: &proto.Event_CompleteEvent{
			CompleteEvent: &proto.EventComplete{
				ExecutionId: c.ExecutionId,
				RowCount:    int64(c.RowCount),
				ChunkCount:  int32(c.ChunksWritten),
				Error:       errString,
			},
		},
	}
}
