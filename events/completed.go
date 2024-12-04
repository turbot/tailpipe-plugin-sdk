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
	Timing        types.TimingCollection
}

func NewCompletedEvent(executionId string, rowCount int, chunksWritten int, timing types.TimingCollection, err error) *Completed {
	return &Completed{
		ExecutionId:   executionId,
		RowCount:      rowCount,
		ChunksWritten: chunksWritten,
		Timing:        timing,
		Err:           err,
	}
}

func (c *Completed) ToProto() *proto.Event {
	errString := ""
	if c.Err != nil {
		errString = c.Err.Error()
	}

	// convert timing map to proto
	protoTimingCollection := TimingCollectionToProto(c.Timing)

	return &proto.Event{
		Event: &proto.Event_CompleteEvent{
			CompleteEvent: &proto.EventComplete{
				ExecutionId: c.ExecutionId,
				RowCount:    int64(c.RowCount),
				ChunkCount:  int32(c.ChunksWritten), //nolint:gosec // TODO look at integer overflow conversion
				Error:       errString,
				Timing:      protoTimingCollection,
			},
		},
	}
}
