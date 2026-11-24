package plugin

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

// AddObserver attaches an event stream: the CLI's, or that of a plugin using this plugin as its source.
// A caller opens a stream and at once starts a collection, and the stream's handler may attach it after the
// collection has sent its first events. So the plugin holds the events it sends while it has no stream, and sends
// them, in order, to the next stream that attaches. A new stream replaces any earlier one, and a stream is retired
// once it is sent a collection's completion event: the CLI, and a plugin using this plugin as its source, open a
// stream for each collection and stop reading it after that collection's completion event.
func (p *PluginImpl) AddObserver(o observable.Observer) error {
	p.eventLock.Lock()
	defer p.eventLock.Unlock()

	if p.spentStreamsToCome > 0 {
		// the stream of a collection that has already failed: its caller no longer reads it
		p.spentStreamsToCome--
		return nil
	}

	p.ObservableImpl.RemoveObservers()
	if err := p.ObservableImpl.AddObserver(o); err != nil {
		return err
	}
	p.hasStream = true

	// send the held events; nothing is held after a completion, so a held completion, which retires the stream, is last
	held := p.heldEvents
	p.heldEvents = nil
	for _, h := range held {
		if err := p.sendLocked(h.ctx, h.event); err != nil {
			slog.Error("error sending a held event to the new event stream", "error", err)
		}
	}
	return nil
}

// NotifyObservers sends the event to the plugin's event stream, or holds it while there is none
func (p *PluginImpl) NotifyObservers(ctx context.Context, e events.Event) error {
	p.eventLock.Lock()
	defer p.eventLock.Unlock()

	if p.completed {
		slog.Debug("dropping an event sent after its collection completed", "event", fmt.Sprintf("%T", e))
		return nil
	}
	if isCompletion(e) {
		p.completed = true
	}
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
	if isCompletion(e) {
		p.retireStreamLocked()
	}
	return err
}

// isCompletion reports whether the event ends a collection: a table's (Complete) or a source's (SourceComplete)
func isCompletion(e events.Event) bool {
	switch e.(type) {
	case *events.Complete, *events.SourceComplete:
		return true
	}
	return false
}

// retireStreamLocked detaches the stream, so later events are held for the next one. The caller holds eventLock.
func (p *PluginImpl) retireStreamLocked() {
	p.ObservableImpl.RemoveObservers()
	p.hasStream = false
	p.streamRetiredSinceBegin = true
}

// collectionBegins is called as a Collect or SourceCollect request arrives: the plugin sends events again. Events
// still held then belong to an earlier collection whose caller has stopped reading, so they are dropped.
func (p *PluginImpl) collectionBegins() {
	p.eventLock.Lock()
	defer p.eventLock.Unlock()
	p.completed = false
	p.streamRetiredSinceBegin = false
	if len(p.heldEvents) > 0 {
		slog.Warn("dropping events held for an earlier collection", "count", len(p.heldEvents))
		p.heldEvents = nil
	}
}

// collectionStarted records that a collection's goroutine is starting: its completion event will retire its stream
func (p *PluginImpl) collectionStarted() {
	p.eventLock.Lock()
	defer p.eventLock.Unlock()
	p.collectionRunning = true
}

// collectionFailed is called when Collect or SourceCollect returns an error, after which the caller no longer reads
// the stream it opened for the collection. With no collection running, no completion event will retire that stream:
// retire it now or, when it has not attached yet, leave it unused when it does. (A stream already retired since the
// collection began was this collection's, retired by its completion.) Either way, drop what the failed collection
// held. A running collection's own completion event retires its stream.
func (p *PluginImpl) collectionFailed() {
	p.eventLock.Lock()
	defer p.eventLock.Unlock()
	if p.collectionRunning {
		return
	}
	switch {
	case p.hasStream:
		p.retireStreamLocked()
	case !p.streamRetiredSinceBegin:
		p.spentStreamsToCome++
	}
	p.heldEvents = nil
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
