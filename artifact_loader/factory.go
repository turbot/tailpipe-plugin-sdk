package artifact_loader

// Factory is a global ArtifactLoaderFactory instance
var Factory = newArtifactLoaderFactory()

type ArtifactLoaderFactory struct {
	artifactLoaders map[string]func() Loader
}

func newArtifactLoaderFactory() ArtifactLoaderFactory {
	return ArtifactLoaderFactory{
		artifactLoaders: make(map[string]func() Loader),
	}
}

func (b *ArtifactLoaderFactory) RegisterArtifactLoaders(loaderFuncs ...func() Loader) {
	for _, ctor := range loaderFuncs {
		// create an instance of the loader to get the identifier
		c := ctor()
		// register the collection
		b.artifactLoaders[c.Identifier()] = ctor
	}
}
