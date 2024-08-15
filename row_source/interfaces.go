package row_source

import (
	"context"
	"encoding/json"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
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
	Init(context.Context, *hcl.Data, ...RowSourceOption) error

	// Identifier must return the source name
	Identifier() string

	Close() error

	// Collect is called to start collecting data,
	Collect(context.Context) error

	// GetPagingDataSchema returns an empty instance of the paging data struct
	// Should be implemented only if paging is supported (RowSourceBase has an empty implementation)
	GetPagingDataSchema() paging.Data
	// GetConfigSchema returns an empty instance of the config struct used by the source
	GetConfigSchema() hcl.Config

	// GetPagingData returns the json serialised paging data for the ongoing collection
	GetPagingData() (json.RawMessage, error)
	// SetPagingData unmarshalls the paging data JSON into the target object
	SetPagingData(pagingDataJSON json.RawMessage) error

	// GetTiming returns the timing for the source row collection
	GetTiming() types.TimingMap
}
