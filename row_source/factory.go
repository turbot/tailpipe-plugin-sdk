package row_source

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
)

// Factory is a global newFactory instance
var Factory = newRowSourceFactoryFactory()

type RowSourceFactory struct {
	sources map[string]func() RowSource
}

func newRowSourceFactoryFactory() RowSourceFactory {
	return RowSourceFactory{
		sources: make(map[string]func() RowSource),
	}
}

// RegisterRowSources registers RowSource implementations
// is should be called by a plugin implementation to register the sources it provides
// it is also called by the base implementation to register the sources the SDK provides
func (b *RowSourceFactory) RegisterRowSources(sourceFunc ...func() RowSource) {
	for _, ctor := range sourceFunc {
		// create an instance of the source to get the identifier
		c := ctor()
		// register the collection
		b.sources[c.Identifier()] = ctor
	}
}

// GetRowSource attempts to instantiate a row source, using the provided row source data
// It will fail if the requested source type is not registered
// Implements [plugin.SourceFactory]
func (b *RowSourceFactory) GetRowSource(ctx context.Context, sourceConfigData *hcl.Data, sourceOpts ...RowSourceOption) (RowSource, error) {
	// look for a constructor for the source
	ctor, ok := b.sources[sourceConfigData.Type]
	if !ok {
		return nil, fmt.Errorf("source not registered: %s", sourceConfigData.Type)
	}
	// create the source
	source := ctor()

	//  register the rowsource implementation with the base struct (_before_ calling Init)
	// create an interface type to use - we do not want to expose this function in the Collection interface
	type BaseCollection interface{ RegisterImpl(rowSource RowSource) }
	baseCol, ok := source.(BaseCollection)
	if !ok {
		return nil, fmt.Errorf("collection implementation must embed collection.Base")
	}
	baseCol.RegisterImpl(source)

	// initialise the source, passing ourselves as source_factory
	if err := source.Init(ctx, sourceConfigData, sourceOpts...); err != nil {
		return nil, fmt.Errorf("failed to initialise source: %w", err)
	}
	return source, nil
}
