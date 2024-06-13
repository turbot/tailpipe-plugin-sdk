package plugin

import (
	"context"
	"errors"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"log"
	"sync"
)

// Base should be embedded in all tailpipe plugin implementations
type Base struct {
	observerLock sync.RWMutex
	// Observers is a list of all Observers that are currently connected
	// for now these are just the GRPC stream corresponding to the AddObserver call
	Observers []EventStream
}

func (t *Base) GetSchema() (*proto.GetSchemaResponse, error) {
	// TODO build JSON schemas from parquet tags
	return nil, nil
}

// AddObserver is the GRPC handler for the AddObserver call
func (p *Base) AddObserver(stream proto.TailpipePlugin_AddObserverServer) error {
	log.Println("[INFO] AddObserver")
	// add to list of Observers
	p.observerLock.Lock()
	p.Observers = append(p.Observers, stream)
	p.observerLock.Unlock()

	// hold stream open
	// TODO do we need a remove observer function, in which case this could wait on a waitgroup associated with the observer?
	select {}
	return nil
}

func (p *Base) NotifyObservers(e Event) error {
	p.observerLock.RLock()
	defer p.observerLock.RUnlock()
	var notifyErrors []error
	for _, observer := range p.Observers {
		ep, err := e.ToProto()
		if err != nil {
			notifyErrors = append(notifyErrors, err)
			continue
		}
		observer.Send(ep)
	}

	return errors.Join(notifyErrors...)
}

func (p *Base) Init(context.Context) error {
	return nil
}

func (p *Base) Shutdown(context.Context) error {
	return nil
}
