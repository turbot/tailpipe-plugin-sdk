package events

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type Status struct {
	Base
	ExecutionId         string
	ArtifactsDiscovered int
	ArtifactsDownloaded int
	ArtifactsExtracted  int
	RowsEnriched        int
	Errors              int
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
				ArtifactsDiscovered: int64(r.ArtifactsDiscovered),
				ArtifactsDownloaded: int64(r.ArtifactsDownloaded),
				ArtifactsExtracted:  int64(r.ArtifactsExtracted),
				RowsEnriched:        int64(r.RowsEnriched),
				Errors:              int32(r.Errors),
			},
		},
	}

}

func (r *Status) Update(event Event) {
	switch event.(type) {
	case *ArtifactDiscovered:
		r.ArtifactsDiscovered++
	case *ArtifactDownloaded:
		r.ArtifactsDownloaded++
	case *ArtifactExtracted:
		r.ArtifactsExtracted++
	case *Error:
		r.Errors++
	case *Row:
		r.RowsEnriched++
	case *Status:
		r.Errors++
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
