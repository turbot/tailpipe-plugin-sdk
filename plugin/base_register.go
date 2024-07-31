package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_row_source"
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
)

type ResourceFunctions struct {
	Collections     []func() collection.Collection
	Sources         []func() row_source.RowSource
	ArtifactSources []func() artifact_row_source.Source
	ArtifactLoaders []func() artifact_loader.Loader
	ArtifactMappers []func() artifact_mapper.Mapper
}

// RegisterResources registers RowSource implementations
// is should be called by a plugin implementation to register the resources it provides
func (b *Base) RegisterResources(resources *ResourceFunctions) error {
	row_source.Factory.RegisterRowSources(resources.Sources...)
	artifact_row_source.Factory.RegisterArtifactSources(resources.ArtifactSources...)
	artifact_loader.Factory.RegisterArtifactLoaders(resources.ArtifactLoaders...)
	artifact_mapper.Factory.RegisterArtifactMappers(resources.ArtifactMappers...)
	// RegisterCollections is the only Register function which returns an error
	return collection.Factory.RegisterCollections(resources.Collections...)
}
