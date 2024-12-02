package table

import (
	"context"
	"encoding/json"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Table is a generic interface representing a plugin table definition
// R is the row struct type
type Table[R types.RowStruct, S parse.Config] interface {
	// Identifier must return the collection name
	Identifier() string

	// GetSourceMetadata returns the supported sources for the table
	GetSourceMetadata(S) []*SourceMetadata[R]
	// EnrichRow is called to enrich the row with common (tp_*) fields
	EnrichRow(row R, sourceEnrichmentFields *enrichment.CommonFields) (R, error)
}

// DynamicTable is a generic interface which provides methods for dynamic tables
type DynamicTable[R types.RowStruct, S parse.Config] interface {
	Init(row_source.RowSource, S) error
	GetSchema() (*schema.RowSchema, error)
}

// ArtifactDynamicTable is a generic interface which provides methods for dynamic tables which are based on artifacts
// (e.g. csv, JSON)
type ArtifactDynamicTable[R types.RowStruct, S parse.DynamicTableConfig] interface {
	DynamicTable[R, S]
	DetermineSchemaFromArtifact(string, S) (*schema.RowSchema, error)
}

// Collection is an interface which provides a methods for collecting table data from a source
// This is implemented by the generic Partition struct
type Collection interface {
	observable.Observable

	GetTiming() types.TimingCollection
	Init(ctx context.Context, request *types.CollectRequest) error
	Identifier() string
	GetSource() row_source.RowSource
	IsDynamic() bool
	Collect(context.Context) (json.RawMessage, error)
	GetSchema() (*schema.RowSchema, error)
}

// Mapper is a generic interface which provides a method for mapping raw source data into row structs
// R is the type of the row struct which the mapperFunc outputs
type Mapper[R types.RowStruct] interface {
	Identifier() string
	// Map converts raw rows to the desired format (type 'R')
	Map(context.Context, any) (R, error)
}

// MapInitialisedRow is an interface which provides a means to initialise a row struct from a string map
// this is used in combination with the RowPatternMapper
type MapInitialisedRow interface {
	InitialiseFromMap(m map[string]string) error
}
