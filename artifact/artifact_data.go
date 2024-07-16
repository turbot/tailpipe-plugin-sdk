package artifact

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
)

type ArtifactData struct {
	Data     any
	Metadata *enrichment.CommonFields
}

type ArtifactDataOption func(*ArtifactData)

func WithMetadata(metadata *enrichment.CommonFields) ArtifactDataOption {
	return func(a *ArtifactData) {
		a.Metadata = metadata
	}
}

func NewData(record any, opts ...ArtifactDataOption) *ArtifactData {
	d := &ArtifactData{
		Data: record,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}
