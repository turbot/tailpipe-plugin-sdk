package artifact

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
)

// ArtifactData is a container for the data and metadata of an artifact
// It is used to pass data the [Loader] and any configured [Mapper]s
// The ArtifactData returned by the final mapper is used as the payload of a [events.Row] which is sent to the
// [plugin.Collection]
type ArtifactData struct {
	Data     any
	Metadata *enrichment.CommonFields
}

// ArtifactDataOption is a function that can be used to set options on an ArtifactData
type ArtifactDataOption func(*ArtifactData)

// WithMetadata is an option that can be used to set the metadata on an ArtifactData
func WithMetadata(metadata *enrichment.CommonFields) ArtifactDataOption {
	return func(a *ArtifactData) {
		a.Metadata = metadata
	}
}

// NewData creates a new ArtifactData, applying the specified [ArtifactDataOption]s
func NewData(record any, opts ...ArtifactDataOption) *ArtifactData {
	d := &ArtifactData{
		Data: record,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}
