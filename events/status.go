package events

import (
	"sync"
	"sync/atomic"

	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Status struct {
	Base
	ExecutionId              string
	LatestArtifactLocation   string // *
	ArtifactsDiscovered      int64
	ArtifactsDownloaded      int64
	ArtifactsDownloadedBytes int64 // *
	ArtifactsExtracted       int64
	ArtifactErrors           int64 // *
	RowsReceived             int64
	RowsEnriched             int64
	Errors                   int64

	// we only need the mutex when updating string fields (i.e. LatestArtifactLocation)
	// we use atomic operations for all int fields
	mut sync.Mutex
}

func NewStatusEvent(executionId string) *Status {
	return &Status{
		ExecutionId: executionId,
	}
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
				ArtifactErrors:           r.ArtifactErrors,
				RowsReceived:             r.RowsReceived,
				RowsEnriched:             r.RowsEnriched,
				Errors:                   r.Errors,
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
	case *Error:
		atomic.AddInt64(&r.Errors, 1)
	}
}
func (r *Status) OnRowEnriched() {
	atomic.AddInt64(&r.RowsEnriched, 1)
}

func (r *Status) Equals(status *Status) bool {
	if status == nil {
		return false
	}

	return r.ArtifactsDiscovered == status.ArtifactsDiscovered &&
		r.ArtifactsDownloaded == status.ArtifactsDownloaded &&
		r.ArtifactsExtracted == status.ArtifactsExtracted &&
		r.RowsEnriched == status.RowsEnriched &&
		r.Errors == status.Errors

}
