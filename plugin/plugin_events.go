package plugin

import (
	"context"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

// AddObserver adds an observer: the GRPC stream of the CLI, or of a plugin using this plugin as its source.
// The first observer is sent, in order, every event the plugin sent before it attached. The caller opens the stream
// and then starts the collection, and the stream's handler may attach it after the collection has sent its first
// events, so without this those events (a source's artifact and completion events among them) would be lost.
func (p *PluginImpl) AddObserver(o observable.Observer) error {
	p.heldLock.Lock()
	defer p.heldLock.Unlock()

	if !p.observed {
		for _, h := range p.heldEvents {
			if err := o.Notify(h.ctx, h.event); err != nil {
				slog.Error("error sending a held event to the new observer", "error", err)
			}
		}
		p.heldEvents = nil
		p.observed = true
	}
	// add the observer before releasing the lock, so an event sent meanwhile reaches it after the held events
	return p.ObservableImpl.AddObserver(o)
}

// NotifyObservers sends the event to the plugin's observers, or holds it until the first observer attaches
func (p *PluginImpl) NotifyObservers(ctx context.Context, e events.Event) error {
	p.heldLock.Lock()
	if !p.observed {
		// keep the context's values but not its cancellation: the event is sent later, when an observer attaches
		p.heldEvents = append(p.heldEvents, heldEvent{ctx: context.WithoutCancel(ctx), event: e})
		p.heldLock.Unlock()
		return nil
	}
	p.heldLock.Unlock()

	return p.ObservableImpl.NotifyObservers(ctx, e)
}

// NotifyError sends an error event to the plugin's observers, or holds it until the first observer attaches
func (p *PluginImpl) NotifyError(ctx context.Context, executionId string, err error) {
	if notifyErr := p.NotifyObservers(ctx, events.NewErrorEvent(executionId, err)); notifyErr != nil {
		slog.Error("error notifying observers of error", "error", notifyErr)
	}
}

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
