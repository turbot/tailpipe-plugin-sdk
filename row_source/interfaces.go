package row_source

import (
	"context"
	"encoding/json"

	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// RowSource is the interface that represents a data source
// A number of data sources are provided by the SDK, and plugins may provide their own
// Built in data sources:
// - AWS S3 Bucket
// - API Source (this must be implemented by the plugin)
// - File Source
// - Webhook source
// Sources may be configured with data transfo
type RowSource interface {
	// Observable must be implemented by row sources (it is implemented by row_source.RowSourceBase)
	observable.Observable

	// Init is called when the row source is created
	// it is responsible for parsing the source config and configuring the source
	Init(context.Context, *parse.Data, ...RowSourceOption) error

	// Identifier must return the source name
	Identifier() string

	Close() error

	// Collect is called to start collecting data,
	Collect(context.Context) error

	// GetConfigSchema returns an empty instance of the config struct used by the source
	GetConfigSchema() parse.Config

	// SetCollectionStateFunc sets the function used to create the collection state data
	SetCollectionStateFunc(func(...collection_state.CollectStateOption) collection_state.CollectionState)
	// TODO needed??
	// SetCollectionStateOpts sets the options to use when creating the collection state data
	SetCollectionStateOpts(opts ...collection_state.CollectStateOption)
	// 	GetCollectionStateJSON() (json.RawMessage, error) returns the json serialised collection state data for the ongoing collection
	GetCollectionStateJSON() (json.RawMessage, error)
	// SetCollectionStateJSON unmarshalls the collection state data JSON into the target object
	SetCollectionStateJSON(stateJSON json.RawMessage) error

	// GetTiming returns the timing for the source row collection
	GetTiming() types.TimingCollection
}
