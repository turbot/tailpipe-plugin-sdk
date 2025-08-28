package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

type SourceMetadata[R any] struct {
	SourceName string

	// ValidateContent allows tables to specify a validation function for artifact content
	// If this function returns false, the artifact will be skipped entirely
	// This validation happens before mapper and options processing
	ValidateContent artifact_source.ContentValidator

	Mapper  mappers.Mapper[R]
	Options []row_source.RowSourceOption
}
