package plugin

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// TailpipePlugin is the interface that all tailpipe plugins must implement
// It is in its own package to avoid circular dependencies as many people need to reference it
type TailpipePlugin interface {
	// Identifier returns the plugin name
	// this must be implemented by the plugin implementation
	Identifier() string

	// Describe returns the duck DB schema for all tables
	// this must be implemented by the plugin implementation
	Describe() (DescribeResponse, error)

	// AddObserver adda an observer to the plugin to receive status events
	// this is implemented by plugin.PluginImpl and should not be overridden
	AddObserver(observable.Observer) error

	// Collect is called to start a collection run
	// this is implemented by plugin.PluginImpl and should not be overridden
	Collect(context.Context, *proto.CollectRequest) (*row_source.ResolvedFromTime, *schema.RowSchema, error)

	// UpdateCollectionState is called to update the collection state
	UpdateCollectionState(ctx context.Context, req *proto.UpdateCollectionStateRequest) error

	// Source functions - used when the plugin is acting as a Source only

	InitSource(context.Context, *proto.InitSourceRequest) (row_source.RowSource, error)
	CloseSource(context.Context) error
	SaveCollectionState(context.Context) error
	SourceCollect(context.Context, *proto.SourceCollectRequest) error

	// Other interface functions

	// Init is implemented by plugin.PluginImpl.
	// If overridden by the plugin it MUST call the base version
	Init(context.Context) error

	// Shutdown is implemented by plugin.PluginImpl (empty implementation)
	// it may be overridden by the plugin
	Shutdown(context.Context) error

	// Impl returns the common plugin implementation - used for validation testing
	Impl() *PluginImpl
}
