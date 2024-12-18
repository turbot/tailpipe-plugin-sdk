package table

import (
	"context"

	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type TableWithFormat[R types.RowStruct, S parse.Config] interface {
	Table[R]
	SetFormat(S)
}

// Table is a generic interface representing a plugin table definition
// R is the row struct type
type Table[R types.RowStruct] interface {
	// Identifier must return the collection name
	Identifier() string

	// GetSourceMetadata returns the supported sources for the table
	GetSourceMetadata() []*SourceMetadata[R]
	// EnrichRow is called to enrich the row with common (tp_*) fields
	EnrichRow(R, enrichment.SourceEnrichment) (R, error)
}

// Collector is an interface which provides a methods for collecting table data from a source
// This is implemented by the generic CollectorImpl struct
type Collector interface {
	observable.Observable

	GetTiming() types.TimingCollection
	Init(ctx context.Context, request *types.CollectRequest) error
	Identifier() string
	Collect(context.Context) (int, int, error)
	GetSchema() (*schema.RowSchema, error)
}

type MapOption[R types.RowStruct] func(Mapper[R])

// Mapper is a generic interface which provides a method for mapping raw source data into row structs
// R is the type of the row struct which the mapperFunc outputs
type Mapper[R types.RowStruct] interface {
	Identifier() string
	// Map converts raw rows to the desired format (type 'R')
	Map(context.Context, any, ...MapOption[R]) (R, error)
}

type SchemaSetter interface {
	SetSchema(*schema.RowSchema)
}

func WithSchema[R types.RowStruct](schema *schema.RowSchema) MapOption[R] {
	return func(m Mapper[R]) {
		if mapper, ok := m.(SchemaSetter); ok {
			mapper.SetSchema(schema)
		}
	}
}

// MapInitialisedRow is an interface which provides a means to initialise a row struct from a string map
// this is used in combination with the GonxMapper/GrokMapper
type MapInitialisedRow interface {
	types.RowStruct
	InitialiseFromMap(m map[string]string) error
}

type ArtifactToJsonConverter[S parse.Config] interface {
	GetArtifactConversionQuery(string, string, S) string
	ArtifactToJSON(context.Context, string, string, int, S) (int, int, error)
}

type ChunkWriter interface {
	WriteChunk(ctx context.Context, rows []any, chunkNumber int) error
}
