package observable

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/events"
)

// ObservableImpl provides a base implementation of the Observable interface
// it should be embedded in all tailpipe plugin, collection and source implementations
// (via collection.RowSourceImpl, source.RowSourceImpl and plugin.RowSourceImpl)
type ObservableImpl struct {
	observerLock sync.RWMutex
	// Observers is a list of all Observers that are currently connected
	// for now these are just the GRPC stream corresponding to the AddObserver call
	Observers []Observer
}

func (p *ObservableImpl) AddObserver(o Observer) error {
	// add to list of Observers
	p.observerLock.Lock()
	p.Observers = append(p.Observers, o)
	p.observerLock.Unlock()

	return nil
}

func (p *ObservableImpl) NotifyObservers(ctx context.Context, e events.Event) error {
	p.observerLock.RLock()
	defer p.observerLock.RUnlock()
	var notifyErrors []error
	for _, observer := range p.Observers {
		err := observer.Notify(ctx, e)
		if err != nil {
			notifyErrors = append(notifyErrors, err)
		}
	}

	return errors.Join(notifyErrors...)
}

func (p *ObservableImpl) NotifyError(ctx context.Context, executionId string, err error) {
	notifyErr := p.NotifyObservers(ctx, events.NewErrorEvent(executionId, err))
	if notifyErr != nil {
		slog.Error("error notifying observers of error", "error", notifyErr)
	}
}
