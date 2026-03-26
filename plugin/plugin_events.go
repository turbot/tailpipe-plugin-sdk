package plugin

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/events"
)

// Notify implements observable.Observer
func (p *PluginImpl) Notify(ctx context.Context, event events.Event) error {
	// notify our observer
	// (NOTE: this will be our GRPC client listening across a GRPC stream)

	// TODO consider throttling events
	// https://github.com/turbot/tailpipe-plugin-sdk/issues/24
	// https://github.com/turbot/tailpipe-plugin-sdk/issues/10

	return p.NotifyObservers(ctx, event)
}

// OnStarted is called by the plugin when it starts processing a collection request
// any observers are notified
func (p *PluginImpl) OnStarted(ctx context.Context, executionId string) error {
	return p.NotifyObservers(ctx, events.NewStartedEvent(executionId))
}

// handleRowEvent is called by the plugin for every row which it produces
// the row is buffered and written to a JSONL file when the buffer is full
