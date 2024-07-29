package plugin

import (
	"errors"
	"github.com/turbot/tailpipe-plugin-sdk/artifact"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

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

func (b *Base) RegisterArtifactSources(sourceFuncs ...func() artifact.Source) error {
	// create the maps if necessary
	if b.artifactSourceFactory == nil {
		b.artifactSourceFactory = make(map[string]func() artifact.Source)
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

func (b *Base) RegisterArtifactMappers(mapperFuncs ...func() artifact.Mapper) error {
	// create the maps if necessary
	if b.artifactMapperFactory == nil {
		b.artifactMapperFactory = make(map[string]func() artifact.Mapper)
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

func (b *Base) RegisterArtifactLoaders(loaderFuncs ...func() artifact.Loader) error {
	// create the maps if necessary
	if b.artifactLoaderFactory == nil {
		b.artifactLoaderFactory = make(map[string]func() artifact.Loader)
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
