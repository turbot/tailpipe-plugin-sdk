package table

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// CustomTable is a generic interface representing a plugin table definition with a format
type CustomTable interface {
	Table[*types.DynamicRow]
	GetSchema() *schema.TableSchema

	Initialize(formats.Format, *schema.TableSchema)
	GetSupportedFormats() *formats.SupportedFormats
	GetTableDefinition() *schema.TableSchema
}

// CustomTable is a generic interface representing a plugin table definition with a format
// TODO KAI GET RID - just use CustomTable?
type PredefinedCustomTable interface {
	CustomTable

	// get the configured format
	GetFormat() formats.Format
}

// Table is a generic interface representing a plugin table definition
// R is the row struct type
type Table[R types.RowStruct] interface {
	// Identifier returns the table name
	Identifier() string

	// GetSourceMetadata returns the supported sources for the table
	GetSourceMetadata() ([]*SourceMetadata[R], error)
	// EnrichRow is called to enrich the row with common (tp_*) fields
	EnrichRow(R, schema.SourceEnrichment) (R, error)
}

// Collector is an interface which provides a methods for collecting table data from a source
// This is implemented by the generic CollectorImpl struct
type Collector interface {
	observable.Observable

	Init(ctx context.Context, request *types.CollectRequest) error
	Identifier() string
	Collect(context.Context) (int, int, error)
	GetSchema() (*schema.TableSchema, error)
	GetFromTime() *row_source.ResolvedFromTime
}

type ArtifactToJsonConverter[S parse.Config] interface {
	GetArtifactConversionQuery(string, string, S) string
	ArtifactToJSON(context.Context, string, string, int, S) (int, int, error)
}

type ChunkWriter interface {
	WriteChunk(ctx context.Context, rows []any, chunkNumber int) error
}
