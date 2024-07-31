package artifact_source

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
)

// Factory is a global ArtifactSourceFactory instance
var Factory = newFactory()

type ArtifactSourceFactory struct {
	artifactSources map[string]func() Source
}

func newFactory() ArtifactSourceFactory {
	return ArtifactSourceFactory{
		artifactSources: make(map[string]func() Source),
	}
}

func (b *ArtifactSourceFactory) RegisterArtifactSources(sourceFuncs ...func() Source) {
	for _, ctor := range sourceFuncs {
		// create an instance of the source to get the identifier
		c := ctor()
		// register the collection
		b.artifactSources[c.Identifier()] = ctor
	}
	return
}

// GetArtifactSource attempts to instantiate an artifact source, using the provided data
// It will fail if the requested source type is not registered
// Implements [plugin.SourceFactory]
func (b *ArtifactSourceFactory) GetArtifactSource(ctx context.Context, sourceConfigData *hcl.Data) (Source, error) {
	// look for a constructor for the source
	ctor, ok := b.artifactSources[sourceConfigData.Type]
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
