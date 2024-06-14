package proto

import (
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/source"
)

func NewRowEvent(row collection.Row, artifact source.Artifact) (*Event, error) {
	r, err := RowToProto(row)
	if err != nil {
		return nil, err

	}
	e := &Event{
		Type: EventType_ROW_EVENT,
		Event: &Event_RowEvent{
			RowEvent: &EventRow{
				Artifact: ArtifactToProto(artifact),
				Row:      r,
			},
		},
	}
	return e, nil
}

func NewCompleteEvent(err error) *Event {
	return &Event{
		Type: EventType_COMPLETE_EVENT,
		Event: &Event_CompleteEvent{
			CompleteEvent: &EventComplete{
				Error: err.Error(),
			},
		},
	}
}
