package observable

import (
	"errors"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"log"
	"sync"
)

// Base should be embedded in all tailpipe plugin/source implementations
type Base struct {
	observerLock sync.RWMutex
	// Observers is a list of all Observers that are currently connected
	// for now these are just the GRPC stream corresponding to the AddObserver call
	Observers []Observer
}

func (p *Base) AddObserver(stream Observer) error {
	log.Println("[INFO] AddObserver")
	// add to list of Observers
	p.observerLock.Lock()
	p.Observers = append(p.Observers, stream)
	p.observerLock.Unlock()

	return nil
}

func (p *Base) NotifyObservers(e events.Event) error {
	p.observerLock.RLock()
	defer p.observerLock.RUnlock()
	var notifyErrors []error
	for _, observer := range p.Observers {
		observer.Notify(e)
	}

	return errors.Join(notifyErrors...)
}
