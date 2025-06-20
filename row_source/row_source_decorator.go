package row_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// RowSourceDecorator is a struct which decorates a RowSource, allowing us to wrap the Collect call, providing a hook to call OnCollectionComplete
type RowSourceDecorator struct {
	rowSource RowSource
}

func NewRowSourceDecorator(rowSource RowSource) RowSource {
	return &RowSourceDecorator{
		rowSource: rowSource,
	}
}

func (r *RowSourceDecorator) AddObserver(observer observable.Observer) error {
	return r.rowSource.AddObserver(observer)
}

func (r *RowSourceDecorator) Pause() error {
	return r.rowSource.Pause()
}

func (r *RowSourceDecorator) PauseProcessingOnly() error {
	return r.rowSource.PauseProcessingOnly()
}

func (r *RowSourceDecorator) Resume() error {
	return r.rowSource.Resume()
}

func (r *RowSourceDecorator) Init(ctx context.Context, params *RowSourceParams, option ...RowSourceOption) error {
	return r.rowSource.Init(ctx, params, option...)
}

func (r *RowSourceDecorator) Identifier() string {
	return r.rowSource.Identifier()
}

func (r *RowSourceDecorator) Description() (string, error) {
	return r.rowSource.Description()
}

func (r *RowSourceDecorator) Properties() map[string]*types.PropertyMetadata {
	return r.rowSource.Properties()
}

func (r *RowSourceDecorator) Close() error {
	return r.rowSource.Close()
}

func (r *RowSourceDecorator) SaveCollectionState() error {
	return r.rowSource.SaveCollectionState()
}

func (r *RowSourceDecorator) Collect(ctx context.Context) error {
	if err := r.rowSource.Collect(ctx); err != nil {
		return err
	}
	// if there was no error, call OnCollectionComplete
	return r.rowSource.OnCollectionComplete()
}

func (r *RowSourceDecorator) OnCollectionComplete() error {
	return r.rowSource.OnCollectionComplete()
}

func (r *RowSourceDecorator) GetFromTime() *ResolvedFromTime {
	return r.rowSource.GetFromTime()
}
