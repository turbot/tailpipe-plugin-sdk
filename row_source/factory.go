package row_source

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

// PluginSourceWrapperIdentifier is the source name for the plugin source wrapper
const PluginSourceWrapperIdentifier = "plugin_source_wrapper"

//func WithPluginReattach(sourcePlugin *types.SourcePluginReattach) RowSourceOption {
//	return func(source RowSource) error {
//		// define interface implemented by the plugin source wrapper
//		type PluginSourceWrapper interface {
//			SetPlugin(sourcePlugin *types.SourcePluginReattach) error
//		}
//		if w, ok := source.(PluginSourceWrapper); ok {
//			return w.SetPlugin(sourcePlugin)
//		}
//		return nil
//	}
//}

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
func (b *RowSourceFactory) GetRowSource(ctx context.Context, params *RowSourceParams, sourceOpts ...RowSourceOption) (RowSource, error) {
	var source RowSource
	sourceType := params.SourceConfigData.Type
	// if a reattach config is provided, we need to create a wrapper source which will handle the reattach
	if params.SourceConfigData.ReattachConfig != nil {
		sourceType = PluginSourceWrapperIdentifier
	}
	//look for a constructor for the source
	ctor, ok := b.sourceFuncs[sourceType]
	if !ok {
		return nil, fmt.Errorf("source not registered: %s", params.SourceConfigData.Type)
	}
	// create the source
	source = ctor()

	// NOTE: register the rowSource implementation with the base struct (_before_ calling Init)
	base, ok := source.(BaseSource)
	if !ok {
		return nil, fmt.Errorf("source implementation must embed row_source.RowSourceImpl")
	}
	base.RegisterSource(source)

	// initialise the source, passing ourselves as source_factory
	if err := source.Init(ctx, params, sourceOpts...); err != nil {
		return nil, fmt.Errorf("failed to initialise source: %w", err)
	}
	return source, nil
}

func (b *RowSourceFactory) GetSources() map[string]func() RowSource {
	return b.sourceFuncs
}

func (b *RowSourceFactory) DescribeSources() (SourceMetadataMap, error) {
	var res = make(SourceMetadataMap)
	for k, f := range b.sourceFuncs {
		source := f()
		desc, err := source.Description()
		if err != nil {
			return nil, fmt.Errorf("failed to get source description: %w", err)
		}
		res[k] = &SourceMetadata{
			Name:        source.Identifier(),
			Description: desc,
		}
	}
	return res, nil
}

func IsArtifactSource(sourceType string) bool {
	// instantiate the source
	if sourceType == constants.ArtifactSourceIdentifier {
		return true
	}

	// TODO hack STRICTLY TEMPORARY https://github.com/turbot/tailpipe-plugin-sdk/issues/67

	// TODO #core how can we tell if any given source is an artifact source?
	// // we need to ask it - either via the local source or if it is tremote we can connect to it and ask

	// we cannot reference artifact_source here as it would create a circular dependency
	// for now use a map of known types
	artifactSources := map[string]struct{}{
		"aws_s3_bucket":      {},
		"file":               {},
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
