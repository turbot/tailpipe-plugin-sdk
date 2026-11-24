package plugin

import (
	"context"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

// AddObserver attaches an event stream: the CLI's, or that of a plugin using this plugin as its source.
// A caller opens a stream and at once starts a collection, and the stream's handler may attach it after the
// collection has sent its first events. So the plugin holds the events it sends while it has no stream, and sends
// them, in order, to the next stream that attaches. A new stream replaces any earlier one, and a stream is retired
// once it is sent a collection's completion event: the CLI opens a stream for each collection, and stops reading it
// after that collection's completion event.
func (p *PluginImpl) AddObserver(o observable.Observer) error {
	p.eventLock.Lock()
	defer p.eventLock.Unlock()

	p.ObservableImpl.RemoveObservers()
	if err := p.ObservableImpl.AddObserver(o); err != nil {
		return err
	}
	p.hasStream = true

	held := p.heldEvents
	p.heldEvents = nil
	for i, h := range held {
		if err := p.sendLocked(h.ctx, h.event); err != nil {
			slog.Error("error sending a held event to the new event stream", "error", err)
		}
		if !p.hasStream {
			// the stream was sent its collection's completion event: hold the rest for the next stream
			p.heldEvents = held[i+1:]
			break
		}
	}
	return nil
}

// NotifyObservers sends the event to the plugin's event stream, or holds it while there is none
func (p *PluginImpl) NotifyObservers(ctx context.Context, e events.Event) error {
	p.eventLock.Lock()
	defer p.eventLock.Unlock()

	if _, ok := e.(*events.Complete); ok {
		p.collectionRunning = false
	}
	if !p.hasStream {
		// keep the context's values but not its cancellation: the event is sent later, when a stream attaches
		p.heldEvents = append(p.heldEvents, heldEvent{ctx: context.WithoutCancel(ctx), event: e})
		return nil
	}
	return p.sendLocked(ctx, e)
}

// NotifyError sends an error event to the plugin's event stream, or holds it while there is none
func (p *PluginImpl) NotifyError(ctx context.Context, executionId string, err error) {
	if notifyErr := p.NotifyObservers(ctx, events.NewErrorEvent(executionId, err)); notifyErr != nil {
		slog.Error("error notifying observers of error", "error", notifyErr)
	}
}

// sendLocked sends the event to the stream; a collection's completion event is the last event its stream is sent.
// The caller holds eventLock.
func (p *PluginImpl) sendLocked(ctx context.Context, e events.Event) error {
	err := p.ObservableImpl.NotifyObservers(ctx, e)
	if _, ok := e.(*events.Complete); ok {
		p.retireStreamLocked()
	}
	return err
}

// retireStreamLocked detaches the stream, so later events are held for the next one. The caller holds eventLock.
func (p *PluginImpl) retireStreamLocked() {
	p.ObservableImpl.RemoveObservers()
	p.hasStream = false
}

// collectionStarted records that a collection's goroutine is starting: its completion event will retire its stream
func (p *PluginImpl) collectionStarted() {
	p.eventLock.Lock()
	defer p.eventLock.Unlock()
	p.collectionRunning = true
}

// collectionFailed is called when Collect returns an error, after which the caller no longer reads the stream it
// opened for the collection. With no collection running, no completion event will retire that stream, so retire it
// now, and drop what the failed collection held. A running collection's own completion event retires its stream.
func (p *PluginImpl) collectionFailed() {
	p.eventLock.Lock()
	defer p.eventLock.Unlock()
	if !p.collectionRunning {
		p.retireStreamLocked()
		p.heldEvents = nil
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
