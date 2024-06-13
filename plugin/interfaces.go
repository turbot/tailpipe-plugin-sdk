package plugin

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
)

// TailpipePlugin is the interface that all tailpipe plugins must implement
type TailpipePlugin interface {
	shared.TailpipePluginServer

	// Init is called when the plugin is started
	// it may be overridden by the plugin - there is an empty implementation in PluginBase
	Init(context.Context) error

	// Shutdown is called when the plugin is stopped
	// it may be overridden by the plugin - there is an empty implementation in PluginBase
	Shutdown(context.Context) error
	// Identifier this must be implemented by the plugin
	Identifier() string
}

// EventStream is the interface that all observers must implement
type EventStream interface {
	Send(*proto.Event) error
}

type Event interface {
	ToProto() *proto.Event
}
