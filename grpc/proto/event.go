package proto

func NewStartedEvent(executionId string) *Event {
	return &Event{
		Event: &Event_StartedEvent{
			StartedEvent: &EventStarted{
				ExecutionId: executionId,
			},
		},
	}
}

func NewCompleteEvent(executionId string, rowCount int, chunkCount int, err error) *Event {
	errString := ""
	if err != nil {
		errString = err.Error()
	}
	return &Event{
		Event: &Event_CompleteEvent{
			CompleteEvent: &EventComplete{
				ExecutionId: executionId,
				RowCount:    int64(rowCount),
				ChunkCount:  int32(chunkCount),
				Error:       errString,
			},
		},
	}
}
