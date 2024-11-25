package table

import (
	"context"
	"encoding/json"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Table is a generic interface representing a plugin table definition
// R is the row struct type
type Table[R types.RowStruct, S parse.Config] interface {
	// Identifier must return the collection name
	Identifier() string

	// SourceMetadata returns the supported sources for the table
	SupportedSources(S) []*SourceMetadata[R]
	// EnrichRow is called to enrich the row with common (tp_*) fields
	EnrichRow(row R, sourceEnrichmentFields *enrichment.CommonFields) (R, error)
}

// Collector is an interface which provides a methods for collecting table data from a source
// This is implemented by the generic Partition struct
type Collector interface {
	observable.Observable

	GetTiming() types.TimingCollection
	Init(ctx context.Context, request *types.CollectRequest) error
	Identifier() string
	GetSchema() (*schema.RowSchema, error)
	Collect(context.Context) (json.RawMessage, error)
}

// Mapper is a generic interface which provides a method for mapping raw source data into row structs
// R is the type of the row struct which the mapper outputs
type Mapper[R types.RowStruct] interface {
	Identifier() string
	// Map converts artifact data to a different format and either return it as rows,
	// or pass it on to the next mapper in the chain
	Map(context.Context, any) ([]R, error)
}

type MapInitialisedRow interface {
	InitialiseFromMap(m map[string]string) error
}
