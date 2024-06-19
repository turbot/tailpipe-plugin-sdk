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

func NewChunkWrittenEvent(executionId string, chunkNumber int) *Event {
	return &Event{
		Event: &Event_ChunkWrittenEvent{
			ChunkWrittenEvent: &EventChunkWritten{
				ExecutionId: executionId,
				ChunkNumber: int32(chunkNumber),
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
