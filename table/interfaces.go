package table

import (
	"context"
	"encoding/json"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Collector interface {
	observable.Observable

	Collect(context.Context, *types.CollectRequest) (json.RawMessage, error)
	GetTiming() types.TimingCollection
}

// Table is a generic interface representing a plugin table
// R is the row struct type
type Table[R types.RowStruct] interface {
	TableCore

	// SourceMetadata returns the supported sources for the table
	SupportedSources() []*SourceMetadata[R]
	// EnrichRow is called to enrich the row with common (tp_*) fields
	EnrichRow(row R, sourceEnrichmentFields *enrichment.CommonFields) (R, error)
}

// TableCore is an interface containing all non generic functions of the Table interface
// we need to split it out so the Factory can use it in its constructor maps (which are not generic)
type TableCore interface {
	// GetCollector returns the collector of the correct generic type
	GetCollector() Collector

	// Init is called when the collection created
	// it is responsible for parsing the config and creating the configured Source
	Init(ctx context.Context, connectionSchemaProvider ConnectionSchemaProvider, req *types.CollectRequest) error
	// Identifier must return the collection name
	Identifier() string
	// GetRowSchema returns an empty instance of the row struct returned by the collection
	GetRowSchema() types.RowStruct
	// GetConfigSchema returns an empty instance of the config struct used by the collection
	GetConfigSchema() parse.Config
}

// ConnectionSchemaProvider is an interface providing a method to return the config schema
// implemented by the plugin
type ConnectionSchemaProvider interface {
	GetConnectionSchema() parse.Config
}

// Mapper is a generic interface which provides a method for mapping raw source data into row structs
// R is the type of the row struct which the mapper outputs
type Mapper[R types.RowStruct] interface {
	Identifier() string
	// Map converts artifact data to a different format and either return it as rows,
	// or pass it on to the next mapper in the chain
	Map(context.Context, any) ([]R, error)
}

// baseTable is an interface which is implemented by the TableImpl of a table
type baseTable interface {
	RegisterImpl(TableCore) error
}

type MapInitialisedRow interface {
	InitialiseFromMap(m map[string]string) error
}
