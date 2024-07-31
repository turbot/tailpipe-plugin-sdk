package artifact_mapper

import (
	"fmt"
)

// Factory is a global ArtifactMapperFactory instance
var Factory = newArtifactMapperFactory()

type ArtifactMapperFactory struct {
	artifactMappers map[string]func() Mapper
}

func newArtifactMapperFactory() ArtifactMapperFactory {
	return ArtifactMapperFactory{
		artifactMappers: make(map[string]func() Mapper),
	}
}

func (b *ArtifactMapperFactory) RegisterArtifactMappers(mapperFuncs ...func() Mapper) {
	for _, ctor := range mapperFuncs {
		// create an instance of the mapper to get the identifier
		c := ctor()
		// register the collection
		b.artifactMappers[c.Identifier()] = ctor
	}
	return
}

// GetArtifactMapper attempts to instantiate an artifact mapper
// It will fail if the requested mapper type is not registered
func (b *ArtifactMapperFactory) GetArtifactMapper(mapperType string) (Mapper, error) {
	// look for a constructor for the mapper
	ctor, ok := b.artifactMappers[mapperType]
	if !ok {
		return nil, fmt.Errorf("mapper not registered: %s", mapperType)
	}
	// create the mapper
	mapper := ctor()

	return mapper, nil
}
