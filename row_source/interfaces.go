package row_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
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
	// Observable must be implemented by row sources (it is implemented by row_source.Base)
	observable.Observable

	// Init is called when the row source is created
	// it is responsible for parsing the source config and configuring the source
	Init(context.Context, *hcl.Data, ...RowSourceOption) error

	// Identifier must return the source name
	Identifier() string

	Close() error

	// Collect is called to start collecting data,
	Collect(context.Context) error

	GetPagingData() paging.Data
}

type SourceFactory interface {
	// GetRowSource attempts to instantiate a row source, using the provided row source data
	// It will fail if the requested source type is not registered
	// this is implemented by plugin.Base and SHOULD NOT be overridden
	GetRowSource(context.Context, *hcl.Data, ...RowSourceOption) (RowSource, error)
}
