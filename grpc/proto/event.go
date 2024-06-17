package proto

func NewStartedEvent() *Event {
	return &Event{
		Type: EventType_STARTED_EVENT,
		Event: &Event_StartedEvent{
			StartedEvent: &EventStarted{},
		},
	}
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
