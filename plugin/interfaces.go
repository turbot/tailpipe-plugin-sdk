package plugin

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"

	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// TailpipePlugin is the interface that all tailpipe plugins must implement
type TailpipePlugin interface {
	// Identifier returns the plugin name
	// this must be implemented by the plugin implementation
	Identifier() string

	// GetSchema returns the duck DB schema for all collections
	// this must be implemented by the plugin implementation
	GetSchema() schema.SchemaMap

	// AddObserver adda an observer to the plugin to receive status events
	// this is implemented by plugin.Base and should not be overridden
	AddObserver(observable.Observer) error

	// Collect is called to start a collection run
	// this is implemented by plugin.Base and should not be overridden
	Collect(context.Context, *proto.CollectRequest) error

	// Other interface functions

	// Init is implemented by plugin.Base.
	// If overridden by the plugin it MUST call the base version
	Init(context.Context) error

	// Shutdown is implemented by plugin.Base (empty implementation)
	// it may be overridden by the plugin
	Shutdown(context.Context) error
}

type SourceFactory interface {
	// GetRowSource attempts to instantiate a row source, using the provided row source data
	// this is implemented by plugin.Base and SHOULD NOT be overridden
	GetRowSource(context.Context, *hcl.Data, ...row_source.RowSourceOption) (row_source.RowSource, error)
}

// Collection is the interface that represents a single schema/'table' provided by a plugin.
// A plugin may support multiple collections
type Collection interface {
	// Observable must be implemented by collections (it is implemented by collection.Base)
	observable.Observable

	// Init is called when the collection created
	// it is responsible for parsing the config and creating the configured Source
	Init(ctx context.Context, sourceFactory SourceFactory, collectionConfigData, sourceConfigData *hcl.Data, sourceOpts ...row_source.RowSourceOption) error
	// Identifier must return the collection name
	Identifier() string
	// SupportedSources returns a list of source names that the collection supports
	SupportedSources() []string
	// GetRowSchema returns an empty instance of the row struct returned by the collection
	GetRowSchema() any
	// GetConfigSchema returns an empty instance of the config struct returned by the collection
	GetConfigSchema() any
	// GetPagingDataSchema returns an empty instance of the paging data struct
	// Should be implemented onl`y if paging is supported (Base bas an empty implementation)
	GetPagingDataSchema() (paging.Data, error)

	// GetSourceOptions returns any options which should be passed to the given source type
	GetSourceOptions(sourceType string) []row_source.RowSourceOption

	// Collect is called to start collecting data,
	// Collect will send enriched rows which satisfy the tailpipe row requirements (todo link/document)
	Collect(context.Context, *proto.CollectRequest) (paging.Data, error)

	// EnrichRow is called for each raw row of data, it must enrich the row and return it
	EnrichRow(row any, sourceEnrichmentFields *enrichment.CommonFields) (any, error)
}

type ChunkWriter interface {
	WriteChunk(ctx context.Context, rows []any, chunkNumber int) error
}
