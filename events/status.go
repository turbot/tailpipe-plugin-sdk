package events

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/error_types"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// how often to send status events

const StatusUpdateInterval = 250 * time.Millisecond

type Status struct {
	Base
	ExecutionId              string
	LatestArtifactLocation   string
	ArtifactsDiscovered      int64
	ArtifactsDownloaded      int64
	ArtifactsDownloadedBytes int64
	ArtifactsExtracted       int64
	SourceErrors             []string
	RowsReceived             int64
	RowsEnriched             int64
	// deprecated
	Errors    int64
	RowErrors *error_types.RowErrors

	// we only need the mutex when updating string fields (i.e. LatestArtifactLocation)
	// we use atomic operations for all int fields
	mut *sync.Mutex
}

func NewStatusEvent(executionId string) *Status {
	return &Status{
		ExecutionId: executionId,
		RowErrors:   error_types.NewRowErrors(),
		mut:         &sync.Mutex{},
	}
}

func StatusFromProto(e *proto.Event) *Status {
	event := e.GetStatusEvent()
	s := &Status{
		LatestArtifactLocation:   event.LatestArtifactPath,
		ArtifactsDiscovered:      event.ArtifactsDiscovered,
		ArtifactsDownloaded:      event.ArtifactsDownloaded,
		ArtifactsDownloadedBytes: event.ArtifactsDownloadedBytes,
		ArtifactsExtracted:       event.ArtifactsExtracted,
		RowsReceived:             event.RowsReceived,
		RowsEnriched:             event.RowsEnriched,
		Errors:                   event.Errors,
		SourceErrors:             event.SourceErrors,
	}
	if event.RowErrors != nil {
		s.RowErrors = error_types.RowErrorsFromProto(event.RowErrors)
	} else {
		s.RowErrors = error_types.NewRowErrors()
	}
	return s
}

func (r *Status) ToProto() *proto.Event {
	return &proto.Event{
		Event: &proto.Event_StatusEvent{
			StatusEvent: &proto.EventStatus{
				LatestArtifactPath:       r.LatestArtifactLocation,
				ArtifactsDiscovered:      r.ArtifactsDiscovered,
				ArtifactsDownloaded:      r.ArtifactsDownloaded,
				ArtifactsDownloadedBytes: r.ArtifactsDownloadedBytes,
				ArtifactsExtracted:       r.ArtifactsExtracted,
				RowsReceived:             r.RowsReceived,
				RowsEnriched:             r.RowsEnriched,
				Errors:                   r.Errors,
				RowErrors:                r.RowErrors.ToProto(),
				SourceErrors:             r.SourceErrors,
			},
		},
	}
}

func (r *Status) Update(event Event) {
	switch t := event.(type) {
	case *ArtifactDiscovered:
		atomic.AddInt64(&r.ArtifactsDiscovered, 1)
		r.mut.Lock()
		r.LatestArtifactLocation = t.Info.Name
		r.mut.Unlock()
	case *ArtifactDownloaded:
		atomic.AddInt64(&r.ArtifactsDownloaded, 1)
		atomic.AddInt64(&r.ArtifactsDownloadedBytes, t.Info.Size)
	case *ArtifactExtracted:
		atomic.AddInt64(&r.ArtifactsExtracted, 1)
	case *RowExtracted:
		atomic.AddInt64(&r.RowsReceived, 1)
	case *ArtifactConverted:
		atomic.AddInt64(&r.ArtifactsExtracted, 1)
		atomic.AddInt64(&r.RowsReceived, t.RowCount)
		atomic.AddInt64(&r.RowsEnriched, t.RowCount)

	case *Error:
		// error events are only raised for source errors (currently)
		r.mut.Lock()
		r.SourceErrors = append(r.SourceErrors, t.Err.Error())
		r.mut.Unlock()
	}
}

func (r *Status) OnRowEnriched() {
	atomic.AddInt64(&r.RowsEnriched, 1)
}

// OnRowError increments the error count and adds the error to the RowErrors
// This happens from CLI side, so no need to pass events
func (r *Status) OnRowError(err error_types.RowError) {
	r.mut.Lock()
	defer r.mut.Unlock()
	r.RowErrors.Add(err)
}
