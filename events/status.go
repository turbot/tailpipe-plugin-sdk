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
	return proto.NewStatusEvent(r.ArtifactsDiscovered, r.ArtifactsDownloaded, r.ArtifactsExtracted, r.RowsEnriched, r.Errors)
}

func (r *Status) Update(event Event) {
	switch event.(type) {
	case *ArtifactDiscovered:
		r.ArtifactsDiscovered++
	case *ArtifactDownloaded:
		r.ArtifactsDownloaded++
	case *ArtifactExtracted:
		r.ArtifactsExtracted++
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
