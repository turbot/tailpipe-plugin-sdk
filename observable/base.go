package observable

import (
	"context"
	"errors"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"log"
	"sync"
)

// RowSourceBase provides a base implementation of the Observable interface
// it should be embedded in all tailpipe plugin, collection and source implementations
// (via collection.RowSourceBase, source.RowSourceBase and plugin.RowSourceBase)

type Base struct {
	observerLock sync.RWMutex
	// Observers is a list of all Observers that are currently connected
	// for now these are just the GRPC stream corresponding to the AddObserver call
	Observers []Observer
}

func (p *Base) AddObserver(o Observer) error {
	log.Println("[INFO] AddObserver")
	// add to list of Observers
	p.observerLock.Lock()
	p.Observers = append(p.Observers, o)
	p.observerLock.Unlock()

	return nil
}

func (p *Base) NotifyObservers(ctx context.Context, e events.Event) error {
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
