package plugin

import (
	"context"
	"encoding/json"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// TailpipePlugin is the interface that all tailpipe plugins must implement
type TailpipePlugin interface {
	// GRPC interface functions

	AddObserver(observable.Observer) error
	Collect(context.Context, *proto.CollectRequest) error

	// GetSchema returns the parquet schema for all collections
	GetSchema() schema.SchemaMap

	// Init is called when the plugin is started
	// it may be overridden by the plugin - there is an empty implementation in Base
	Init(context.Context) error

	// Shutdown is called when the plugin is stopped
	// it may be overridden by the plugin - there is an empty implementation in Base
	Shutdown(context.Context) error

	// Identifier must return the plugin name
	Identifier() string
}

// Collection is the interface that represents a single schema/'table' provided by a plugin.
// A plugin may support multiple collections
type Collection interface {
	// Observable must be implemented by collections (it is implemented by collection.Base)
	observable.Observable

	Init(ctx context.Context, config []byte) error
	// Identifier must return the collection name
	Identifier() string

	// Collect is called to start collecting data,
	// it accepts a RowPublisher that will be called for each row of data
	// Collect will send enriched rows which satisfy the tailpipe row requirements (todo link/document
	Collect(context.Context, *proto.CollectRequest) error
	// GetRowStruct returns an instance of the row struct returned by the collection
	GetRowStruct() any

	// GetPagingData converts json into a paging data struct
	GetPagingData(json json.RawMessage) (paging.Data, error)
	// NewPagingData returns a new empty paging data struct
	NewPagingData() (paging.Data, error)

	// EnrichRow is called for each raw row of data, it must enrich the row and return it
	EnrichRow(row any, sourceEnrichmentFields *enrichment.CommonFields) (any, error)
}

// RowSource is the interface that represents a data source
// A number of data sources are provided by the SDK, and plugins may provide their own
// Built in data sources:
// - AWS S3 Bucket
// - API Source (this must be implemented by the plugin)
// - File Source
// - Webhook source
// Sources may be configured with data transfo
type RowSource interface {
	// Observable must be implemented by row sources (it is implemented by row_source.Base)
	observable.Observable

	Close() error

	// Collect is called to start collecting data,
	Collect(context.Context) error
}

type ChunkWriter interface {
	WriteChunk(ctx context.Context, rows []any, chunkNumber int) error
}
