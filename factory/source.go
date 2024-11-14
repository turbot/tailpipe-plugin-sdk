package factory

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

// Factory is a global newFactory instance
var Source = newRowSourceFactoryFactory()

type RowSourceFactory struct {
	sourceFuncs map[string]func() row_source.RowSource
}

func newRowSourceFactoryFactory() RowSourceFactory {
	return RowSourceFactory{
		sourceFuncs: make(map[string]func() row_source.RowSource),
	}
}

// RegisterRowSource registers RowSource implementations
// is should be called by the package init function of row source implementations
func (b *RowSourceFactory) RegisterRowSource(ctor func() row_source.RowSource) {
	// create an instance of the source to get the identifier
	c := ctor()
	// register the source
	b.sourceFuncs[c.Identifier()] = ctor
}

// GetRowSource attempts to instantiate a row source, using the provided row source data
// It will fail if the requested source type is not registered
// Implements [plugin.SourceFactory]
func (b *RowSourceFactory) GetRowSource(ctx context.Context, sourceConfigData *config_data.SourceConfigData, sourceOpts ...row_source.RowSourceOption) (row_source.RowSource, error) {
	// look for a constructor for the source
	ctor, ok := b.sourceFuncs[sourceConfigData.Type]
	if !ok {
		return nil, fmt.Errorf("source not registered: %s", sourceConfigData.Type)
	}
	// create the source
	source := ctor()

	//  register the rowsource implementation with the base struct (_before_ calling Init)
	// create an interface type to use - we do not want to expose this function in the RowSource interface
	type baseSource interface {
		RegisterSource(rowSource row_source.RowSource)
	}
	base, ok := source.(baseSource)
	if !ok {
		return nil, fmt.Errorf("source implementation must embed row_source.RowSourceImpl")
	}
	base.RegisterSource(source)

	// initialise the source, passing ourselves as source_factory
	if err := source.Init(ctx, sourceConfigData, sourceOpts...); err != nil {
		return nil, fmt.Errorf("failed to initialise source: %w", err)
	}
	return source, nil
}

func (b *RowSourceFactory) GetSources() map[string]func() row_source.RowSource {
	return b.sourceFuncs
}

func (b *RowSourceFactory) IsArtifactSource(sourceType string) (bool, error) {
	// instantiate the source
	if sourceType == constants.ArtifactSourceIdentifier {
		return true, nil
	}

	sourceFunc := b.sourceFuncs[sourceType]
	if sourceFunc() == nil {
		return false, fmt.Errorf("source not registered: %s", sourceType)
	}
	source := sourceFunc()
	_, ok := source.(artifact_source.ArtifactSource)
	return ok, nil
}
