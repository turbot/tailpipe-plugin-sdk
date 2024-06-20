package plugin

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
)

// TailpipePlugin is the interface that all tailpipe plugins must implement
type TailpipePlugin interface {
	// TailpipePluginServer is the embedded GRPC server interface
	shared.TailpipePluginServer

	// Init is called when the plugin is started
	// it may be overridden by the plugin - there is an empty implementation in Base
	Init(context.Context) error

	// Shutdown is called when the plugin is stopped
	// it may be overridden by the plugin - there is an empty implementation in Base
	Shutdown(context.Context) error

	// Identifier must return the plugin name
	Identifier() string
}

// RowPublisher is the interface that all plugins must implement to publish rows
type RowPublisher interface {
	OnRow(any, *proto.CollectRequest) error
}

// RowEnricher muyst be implemented by collections - it is called with raw rows, itr must enrich them
// and send to their publisher
type RowEnricher interface {
	OnRow(row any, connection string, req *proto.CollectRequest) error
}

// EventStream is the interface that all observers must implement
type EventStream interface {
	Send(*proto.Event) error
}

// Collection is the interface that represents a single schema/'table' provided by a plugin.
// A plugin may support multiple collections
type Collection interface {
	// Collect is called to start collecting data,
	// it accepts a RowPublisher that will be called for each row of data
	// Collect will send enriched rows which satisfy the tailpipe row requirements (todo link/document
	Collect(context.Context, *proto.CollectRequest) error
}

// Source is the interface that represents a data source
// A number of data sources are provided by the SDK, and plugins may provide their own
// Built in data sources:
// - AWS S3 Bucket
// - API Source (this must be implemented by the plugin)
// - File Source
// - Webhook source
// Sources may be configured with data transfo
type Source interface {
	// Collect is called to start collecting data,
	// it accepts a RowEnricher that will be called for each raw row of data
	// Collect will send raw rows which will need enriching by the collection
	Collect(context.Context, *proto.CollectRequest) error
}
