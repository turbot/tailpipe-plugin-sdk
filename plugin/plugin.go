package plugin

import (
	"context"

	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/source"
)

type Plugin interface {
	Identifier() string
	Init(context.Context) error
	Context() context.Context
	AddObserver(PluginObserver)
	RemoveObserver(PluginObserver)
	Sources() map[string]source.Plugin
	Collections() map[string]collection.Plugin
}
