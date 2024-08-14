package proto

import "encoding/json"

func NewStartedEvent(executionId string) *Event {
	return &Event{
		Event: &Event_StartedEvent{
			StartedEvent: &EventStarted{
				ExecutionId: executionId,
			},
		},
	}
}

func NewChunkWrittenEvent(executionId string, chunkNumber int, pagingData json.RawMessage) *Event {
	return &Event{
		Event: &Event_ChunkWrittenEvent{
			ChunkWrittenEvent: &EventChunkWritten{
				ExecutionId: executionId,
				ChunkNumber: int32(chunkNumber),
				PagingData:  pagingData,
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

func NewErrorEvent(executionId string, err error) *Event {
	return &Event{
		Event: &Event_ErrorEvent{
			ErrorEvent: &EventError{
				ExecutionId: executionId,
				Error:       err.Error(),
			},
		},
	}
}

func NewStatusEvent(artifactsDiscovered, artifactsDownloaded, artifactsExtracted, rowsEnriched int, errors int) *Event {
	return &Event{
		Event: &Event_StatusEvent{
			StatusEvent: &EventStatus{
				ArtifactsDiscovered: int64(artifactsDiscovered),
				ArtifactsDownloaded: int64(artifactsDownloaded),
				ArtifactsExtracted:  int64(artifactsExtracted),
				RowsEnriched:        int64(rowsEnriched),
				Errors:              int32(errors),
			},
		},
	}
}
