package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
)

// RowData is a container for the data and metadata of an row
// It is used to pass data the [Loader]
// The RowData returned by the loader  is used as the payload of a [events.Row] which is sent to the [table.Table]
type RowData struct {
	Data     any
	Metadata *enrichment.CommonFields
}

// ArtifactDataOption is a function that can be used to set options on an RowData
type ArtifactDataOption func(*RowData)

// WithMetadata is an option that can be used to set the metadata on an RowData
func WithMetadata(metadata *enrichment.CommonFields) ArtifactDataOption {
	return func(a *RowData) {
		a.Metadata = metadata
	}
}

// NewData creates a new RowData, applying the specified [ArtifactDataOption]s
func NewData(record any, opts ...ArtifactDataOption) *RowData {
	d := &RowData{
		Data: record,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}
