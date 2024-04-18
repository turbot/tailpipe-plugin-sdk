package collection

import (
	"context"

	"github.com/turbot/tailpipe-plugin-sdk/source"
)

type Plugin interface {
	Identifier() string
	Init(context.Context) error
	Context() context.Context
	AddObserver(CollectionObserver)
	RemoveObserver(CollectionObserver)
	LoadConfig(raw []byte) error
	ValidateConfig() error
	ExtractArtifactRows(context.Context, *source.Artifact) error
	Schema() Row
}
