package partition

import (
	"context"
	"encoding/json"

	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Partition is the interface that represents a single schema/'table' provided by a plugin.
// A plugin may support multiple partitions
type Partition interface {
	// Observable must be implemented by partitionFuncs (it is implemented by partition.PartitionBase)
	observable.Observable

	// Init is called when the collection created
	// it is responsible for parsing the config and creating the configured Source
	Init(ctx context.Context, partitionConfigData *parse.Data, collectionStateJSON json.RawMessage, sourceConfigData *parse.Data) error
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
	Collect(context.Context, *proto.CollectRequest) (json.RawMessage, error)
	// EnrichRow is called for each raw row of data, it must enrich the row and return it
	EnrichRow(row any, sourceEnrichmentFields *enrichment.CommonFields) (any, error)
	// GetTiming returns the timing for the collection
	GetTiming() types.TimingCollection
}
