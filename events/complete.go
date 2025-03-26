package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Complete struct {
	Base
	ExecutionId   string
	RowCount      int64
	ChunksWritten int32
	Err           error
}

func NewCompletedEvent(executionId string, rowCount int64, chunksWritten int32, err error) *Complete {
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
				RowCount:    c.RowCount,
				ChunkCount:  c.ChunksWritten,
				Error:       errString,
			},
		},
	}
}
