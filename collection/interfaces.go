package collection

import (
	"context"

	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

// Collection is the interface that represents a single schema/'table' provided by a plugin.
// A plugin may support multiple collections
type Collection interface {
	// Observable must be implemented by collections (it is implemented by collection.Base)
	observable.Observable

	// Init is called when the collection created
	// it is responsible for parsing the config and creating the configured Source
	Init(ctx context.Context, collectionConfigData, sourceConfigData *hcl.Data, sourceOpts ...row_source.RowSourceOption) error
	// Identifier must return the collection name
	Identifier() string
	// SupportedSources returns a list of source names that the collection supports
	SupportedSources() []string
	// GetRowSchema returns an empty instance of the row struct returned by the collection
	GetRowSchema() any
	// GetConfigSchema returns an empty instance of the config struct returned by the collection
	GetConfigSchema() any
	// GetSourceOptions returns any options which should be passed to the given source type
	GetSourceOptions(sourceType string) []row_source.RowSourceOption

	// Collect is called to start collecting data,
	// Collect will send enriched rows which satisfy the tailpipe row requirements (todo link/document)
	Collect(context.Context, *proto.CollectRequest) (paging.Data, error)

	// EnrichRow is called for each raw row of data, it must enrich the row and return it
	EnrichRow(row any, sourceEnrichmentFields *enrichment.CommonFields) (any, error)
}
