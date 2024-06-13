package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
)

type Plugin interface {
	shared.TailpipePluginServer
	//Identifier() string
	//Init(context.Context) error
	//AddObserver(PluginObserver)
	//RemoveObserver(PluginObserver)
	//Sources() map[string]source.Plugin
	//Collections() map[string]collection.Plugin
}
