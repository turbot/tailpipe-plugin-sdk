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

// CustomTable is an interface representing a plugin table definition with a format
type CustomTable interface {
	Table[*types.DynamicRow]
	Initialize(formats.Format, *schema.TableSchema) error
	GetSchema() *schema.TableSchema
	GetCustomSchema() *schema.TableSchema
	GetDefaultFormat() formats.Format
	GetTableDefinition() *schema.TableSchema
	GetFormat() formats.Format
}

// Table is a generic interface representing a plugin table definition
// R is the row struct type
type Table[R any] interface {
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
	observable.PausableObservable

	Init(ctx context.Context, request *types.CollectRequest) error
	Identifier() string
	Collect(context.Context) (int64, int32, error)
	GetSchema() (*schema.TableSchema, error)
	GetFromTime() *row_source.ResolvedFromTime
	Close()
}

type ArtifactToJsonConverter[S parse.Config] interface {
	GetArtifactConversionQuery(string, string, S) string
	ArtifactToJSON(context.Context, string, string, int, S) (int, int, error)
}

type ChunkWriter interface {
	WriteChunk(ctx context.Context, rows []any, chunkNumber int32) error
}
