package table

import (
	"context"
	"encoding/json"

	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Table is the interface that represents a single schema/'table' provided by a plugin.
// A plugin may support multiple tables
type Table interface {
	// Observable must be implemented by tableFuncs (it is implemented by table.TableImpl)
	observable.Observable

	// Init is called when the collection created
	// it is responsible for parsing the config and creating the configured Source
	Init(ctx context.Context, connectionSchemaProvider ConnectionSchemaProvider, req *types.CollectRequest) error
	// Identifier must return the collection name
	Identifier() string
	// GetRowSchema returns an empty instance of the row struct returned by the collection
	GetRowSchema() any
	// GetConfigSchema returns an empty instance of the config struct used by the collection
	GetConfigSchema() parse.Config
	// GetSourceOptions returns any options which should be passed to the given source type
	GetSourceOptions(sourceType string) []row_source.RowSourceOption

	// Collect is called to start collecting data,
	// Collect will send enriched rows which satisfy the tailpipe row requirements
	Collect(context.Context, *types.CollectRequest) (json.RawMessage, error)
	// GetTiming returns the timing for the collection
	GetTiming() types.TimingCollection
}

// ConnectionSchemaProvider is an interface that is implemented by th eplugin which provides the config schema
type ConnectionSchemaProvider interface {
	GetConnectionSchema() parse.Config
}

// Enricher is a generic interface implemented by tables
// separate from Table interface to avoid Table needing to be generic
// (which breaks the table factory implementation)
type Enricher[T any] interface {
	Table
	EnrichRow(row T, sourceEnrichmentFields *enrichment.CommonFields) (T, error)
}
