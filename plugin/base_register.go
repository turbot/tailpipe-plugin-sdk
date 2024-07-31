package plugin

import (
	"context"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// TODO maybe break out a 'factory' struct to handle all of this

// RegisterSources registers RowSource implementations
// is should be called by a plugin implementation to register the sources it provides
// it is also called by the base implementation to register the sources the SDK provides
func (b *Base) RegisterSources(sourceFunc ...func() row_source.RowSource) error {
	// create the maps if necessary
	if b.sourceFactory == nil {
		b.sourceFactory = make(map[string]func() row_source.RowSource)
	}

	errs := make([]error, 0)
	for _, ctor := range sourceFunc {
		// create an instance of the source to get the identifier
		c := ctor()
		// register the collection
		b.sourceFactory[c.Identifier()] = ctor
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (b *Base) RegisterCollections(collectionFunc ...func() Collection) error {
	// create the maps
	b.collectionFactory = make(map[string]func() Collection)
	b.schemaMap = make(map[string]*schema.RowSchema)

	errs := make([]error, 0)
	for _, ctor := range collectionFunc {
		// create an instance of the collection to get the identifier
		c := ctor()
		// register the collection
		b.collectionFactory[c.Identifier()] = ctor

		// get the schema for the collection row type
		rowStruct := c.GetRowSchema()
		s, err := schema.SchemaFromStruct(rowStruct)
		if err != nil {
			errs = append(errs, err)
		}
		// merge in the common schema
		b.schemaMap[c.Identifier()] = s
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (b *Base) RegisterArtifactSources(sourceFuncs ...func() artifact_source.Source) error {
	// create the maps if necessary
	if b.artifactSourceFactory == nil {
		b.artifactSourceFactory = make(map[string]func() artifact_source.Source)
	}

	errs := make([]error, 0)
	for _, ctor := range sourceFuncs {
		// create an instance of the source to get the identifier
		c := ctor()
		// register the collection
		b.artifactSourceFactory[c.Identifier()] = ctor
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (b *Base) RegisterArtifactMappers(mapperFuncs ...func() artifact_mapper.Mapper) error {
	// create the maps if necessary
	if b.artifactMapperFactory == nil {
		b.artifactMapperFactory = make(map[string]func() artifact_mapper.Mapper)
	}

	errs := make([]error, 0)
	for _, ctor := range mapperFuncs {
		// create an instance of the source to get the identifier
		c := ctor()
		// register the collection
		b.artifactMapperFactory[c.Identifier()] = ctor
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (b *Base) RegisterArtifactLoaders(loaderFuncs ...func() artifact_loader.Loader) error {
	// create the maps if necessary
	if b.artifactLoaderFactory == nil {
		b.artifactLoaderFactory = make(map[string]func() artifact_loader.Loader)
	}

	errs := make([]error, 0)
	for _, ctor := range loaderFuncs {
		// create an instance of the source to get the identifier
		c := ctor()
		// register the collection
		b.artifactLoaderFactory[c.Identifier()] = ctor
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// GetRowSource attempts to instantiate a row source, using the provided row source data
// It will fail if the requested source type is not registered
// Implements [plugin.SourceFactory]
func (b *Base) GetRowSource(ctx context.Context, sourceConfigData *hcl.Data, sourceOpts ...row_source.RowSourceOption) (row_source.RowSource, error) {
	// look for a constructor for the source
	ctor, ok := b.sourceFactory[sourceConfigData.Type]
	if !ok {
		return nil, fmt.Errorf("source not registered: %s", sourceConfigData.Type)
	}
	// create the source
	source := ctor()

	// if this is an artifact row source, pass an option the source factory property
	if sourceConfigData.Type == artifact.ArtifactRowSourceIdentifier {
		sourceOpts = append(sourceOpts, artifact.WithSourceFactory(b))
	}

	// initialise the source, passing ourselves as source_factory
	if err := source.Init(ctx, sourceConfigData, sourceOpts...); err != nil {
		return nil, fmt.Errorf("failed to initialise source: %w", err)
	}
	return source, nil
}

// GetArtifactSource attempts to instantiate an artifact source, using the provided data
// It will fail if the requested source type is not registered
// Implements [plugin.SourceFactory]
func (b *Base) GetArtifactSource(ctx context.Context, sourceConfigData *hcl.Data) (artifact_source.Source, error) {
	// look for a constructor for the source
	ctor, ok := b.artifactSourceFactory[sourceConfigData.Type]
	if !ok {
		return nil, fmt.Errorf("source not registered: %s", sourceConfigData.Type)
	}
	// create the source
	source := ctor()

	// initialise the artifact source
	if err := source.Init(ctx, sourceConfigData); err != nil {
		return nil, fmt.Errorf("failed to initialise source: %w", err)
	}
	return source, nil
}
