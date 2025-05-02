package events

import (
	"fmt"

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

func CompleteFromProto(e *proto.Event) Event {
	event := e.GetCompleteEvent()

	res := &Complete{
		ExecutionId:   event.ExecutionId,
		RowCount:      event.RowCount,
		ChunksWritten: event.ChunkCount,
	}
	if event.Error != "" {
		res.Err = fmt.Errorf(event.Error)
	}
	return res
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
