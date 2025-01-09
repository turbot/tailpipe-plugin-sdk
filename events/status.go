package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"sync/atomic"
)

type Status struct {
	Base
	ExecutionId         string
	ArtifactsDiscovered int64
	ArtifactsDownloaded int64
	ArtifactsExtracted  int64
	RowsEnriched        int64
	Errors              int32
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
				ArtifactsDiscovered: r.ArtifactsDiscovered,
				ArtifactsDownloaded: r.ArtifactsDownloaded,
				ArtifactsExtracted:  r.ArtifactsExtracted,
				RowsEnriched:        r.RowsEnriched,
				Errors:              r.Errors,
			},
		},
	}
}

func (r *Status) Update(event Event) {
	switch event.(type) {
	case *ArtifactDiscovered:
		atomic.AddInt64(&r.ArtifactsDiscovered, 1)
	case *ArtifactDownloaded:
		atomic.AddInt64(&r.ArtifactsDownloaded, 1)
	case *ArtifactExtracted:
		atomic.AddInt64(&r.ArtifactsExtracted, 1)
	case *Error:
		atomic.AddInt32(&r.Errors, 1)
	case *Row:
		atomic.AddInt64(&r.RowsEnriched, 1)
	}
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
