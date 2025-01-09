package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// SourceComplete is an event that is fired when a plugin used a source has completed a source collection
type SourceComplete struct {
	Base
	ExecutionId string
	Err         error
	Timing      types.TimingCollection
}

func NewSourceCompletedEvent(executionId string, timing types.TimingCollection, err error) *SourceComplete {
	return &SourceComplete{
		ExecutionId: executionId,
		Timing:      timing,
		Err:         err,
	}
}

func (c *SourceComplete) ToProto() *proto.Event {
	errString := ""
	if c.Err != nil {
		errString = c.Err.Error()
	}

	// convert timing map to proto
	protoTimingCollection := TimingCollectionToProto(c.Timing)

	return &proto.Event{
		Event: &proto.Event_SourceCompleteEvent{
			SourceCompleteEvent: &proto.EventSourceComplete{
				ExecutionId: c.ExecutionId,
				Error:       errString,
				Timing:      protoTimingCollection,
			},
		},
	}
}

func SourceCompleteFromProto(e *proto.Event) Event {
	return &SourceComplete{
		ExecutionId: e.GetCompleteEvent().ExecutionId,
		Err:         nil,
		Timing:      TimingCollectionFromProto(e.GetCompleteEvent().Timing),
	}
}
