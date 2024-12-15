package row_source

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

// RegisterRowSource registers a row source type
// this is called from the package init function of the table implementation
func RegisterRowSource[T RowSource]() {
	tableFunc := func() RowSource {
		return utils.InstanceOf[T]()
	}
	Factory.registerRowSource(tableFunc)
}

// Factory is a global newFactory instance
var Factory = newRowSourceFactoryFactory()

type RowSourceFactory struct {
	sourceFuncs map[string]func() RowSource
}

func newRowSourceFactoryFactory() RowSourceFactory {
	return RowSourceFactory{
		sourceFuncs: make(map[string]func() RowSource),
	}
}

// RegisterRowSource registers RowSource implementations
// is should be called by the package init function of row source implementations
func (b *RowSourceFactory) registerRowSource(ctor func() RowSource) {
	// create an instance of the source to get the identifier
	c := ctor()
	// register the source
	b.sourceFuncs[c.Identifier()] = ctor
}

// GetRowSource attempts to instantiate a row source, using the provided row source data
// It will fail if the requested source type is not registered
// Implements [plugin.SourceFactory]
func (b *RowSourceFactory) GetRowSource(ctx context.Context, sourceConfigData *config_data.SourceConfigData, connectionData *config_data.ConnectionConfigData, sourceOpts ...RowSourceOption) (RowSource, error) {
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
		RegisterSource(rowSource RowSource)
	}
	base, ok := source.(baseSource)
	if !ok {
		return nil, fmt.Errorf("source implementation must embed row_source.RowSourceImpl")
	}
	base.RegisterSource(source)

	// initialise the source, passing ourselves as source_factory
	if err := source.Init(ctx, sourceConfigData, connectionData, sourceOpts...); err != nil {
		return nil, fmt.Errorf("failed to initialise source: %w", err)
	}
	return source, nil
}

func (b *RowSourceFactory) GetSources() map[string]func() RowSource {
	return b.sourceFuncs
}

func (b *RowSourceFactory) DescribeSources() SourceMetadataMap {
	var res = make(SourceMetadataMap)
	for k, f := range b.sourceFuncs {
		source := f()
		res[k] = &SourceMetadata{
			Name:        source.Identifier(),
			Description: source.Description(),
		}
	}
	return res
}

func IsArtifactSource(sourceType string) bool {
	// instantiate the source
	if sourceType == constants.ArtifactSourceIdentifier {
		return true
	}

	// TODO K hack STRICTLY TEMPORARY
	// we cannot reference artifact_source here as it would create a circular dependency
	// for now use a map of known types
	artifactSources := map[string]struct{}{
		"aws_s3_bucket":      {},
		"file_system":        {},
		"gcp_storage_bucket": {},
	}
	_, ok := artifactSources[sourceType]
	return ok

	//
	//sourceFunc := b.sourceFuncs[sourceType]
	//if sourceFunc() == nil {
	//	return false, fmt.Errorf("source not registered: %s", sourceType)
	//}
	//source := sourceFunc()
	//_, ok := source.(artifact_source.ArtifactSource)
	//return ok
}
