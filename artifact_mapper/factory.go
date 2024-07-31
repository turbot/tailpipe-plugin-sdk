package artifact_mapper

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
