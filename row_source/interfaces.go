package row_source

import (
	"context"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// RowSource is the interface that represents a data source
// A number of data sourceFuncs are provided by the SDK, and plugins may provide their own
// Built in data sourceFuncs:
// - AWS S3 Bucket
// - API Source (this must be implemented by the plugin)
// - File Source
// - Webhook source
// Sources may be configured with data transfo
type RowSource interface {
	// Observable must be implemented by row sourceFuncs (it is implemented by row_source.RowSourceImpl)
	observable.Observable

	// Init is called when the row source is created
	// it is responsible for parsing the source config and configuring the source
	Init(context.Context, *RowSourceParams, ...RowSourceOption) error

	// Identifier must return the source name
	Identifier() string

	// Description returns a human readable description of the source
	Description() (string, error)

	Close() error

	SaveCollectionState() error

	// Collect is called to start collecting data,
	Collect(context.Context) error

	// SetFromTime sets the start time for the data collection
	SetFromTime(from time.Time)

	// GetTiming returns the timing for the source row collection
	GetTiming() (types.TimingCollection, error)

	// GetFromTime returns the start time for the data collection, including the source of the from time
	// (config, collection state or default)
	GetFromTime() *ResolvedFromTime
}

// BaseSource registers the rowSource implementation with the base struct (_before_ calling Init)
// we do not want to expose this function in the RowSource interface
type BaseSource interface {
	RegisterSource(rowSource RowSource)
}
