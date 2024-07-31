package plugin

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// TailpipePlugin is the interface that all tailpipe plugins must implement
// It is in its own package to avoid circular dependencies as many people need to reference it
type TailpipePlugin interface {
	// Identifier returns the plugin name
	// this must be implemented by the plugin implementation
	Identifier() string

	// GetSchema returns the duck DB schema for all collections
	// this must be implemented by the plugin implementation
	GetSchema() schema.SchemaMap

	// AddObserver adda an observer to the plugin to receive status events
	// this is implemented by plugin.Base and should not be overridden
	AddObserver(observable.Observer) error

	// Collect is called to start a collection run
	// this is implemented by plugin.Base and should not be overridden
	Collect(context.Context, *proto.CollectRequest) error

	// Other interface functions

	// Init is implemented by plugin.Base.
	// If overridden by the plugin it MUST call the base version
	Init(context.Context) error

	// Shutdown is implemented by plugin.Base (empty implementation)
	// it may be overridden by the plugin
	Shutdown(context.Context) error
}

type ChunkWriter interface {
	WriteChunk(ctx context.Context, rows []any, chunkNumber int) error
}
