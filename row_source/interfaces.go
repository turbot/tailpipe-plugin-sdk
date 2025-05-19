package row_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/types"

	"github.com/turbot/tailpipe-plugin-sdk/observable"
)
type RowSourceDecorator struct{
	rowSource RowSource
}


func (r RowSourceDecorator) AddObserver(observer observable.Observer) error {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) Pause() error {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) PauseProcessingOnly() error {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) Resume() error {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) Init(ctx context.Context, params *RowSourceParams, option ...RowSourceOption) error {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) Identifier() string {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) Description() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) Properties() map[string]*types.PropertyMetadata {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) Close() error {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) SaveCollectionState() error {
	//TODO implement me
	panic("implement me")
}

func (r RowSourceDecorator) Collect(ctx context.Context) error {
	err := r.rowSource.Collect(ctx)
	return r.rowSource.OnCollectionComplete(err)
}

func (r RowSourceDecorator) GetFromTime() *ResolvedFromTime {
	//TODO implement me
	panic("implement me")
}
{}
// RowSource is the interface that represents a data source
// A number of data sourceFuncs are provided by the SDK, and plugins may provide their own
// Built in data sourceFuncs:
// - AWS S3 Bucket
// - API Source (this must be implemented by the plugin)
// - File Source
// - Webhook source
// Sources may be configured with data transfo
type RowSource interface {
	observable.PausableObservable

	// Init is called when the row source is created
	// it is responsible for parsing the source config and configuring the source
	Init(context.Context, *RowSourceParams, ...RowSourceOption) error

	// Identifier must return the source name
	Identifier() string

	// Description returns a human readable description of the source
	Description() (string, error)

	// Properties returns a map of property descriptions
	// this is used for introspection
	Properties() map[string]*types.PropertyMetadata

	Close() error

	SaveCollectionState() error

	// Collect is called to start collecting data,
	Collect(context.Context) error

	// GetFromTime returns the start time for the data collection, including the source of the from time
	// (config, collection state or default)
	GetFromTime() *ResolvedFromTime

	OnCollectionComplete(error)error
}

// BaseSource registers the rowSource implementation with the base struct (_before_ calling Init)
// we do not want to expose this function in the RowSource interface
type BaseSource interface {
	RegisterSource(rowSource RowSource)
}
