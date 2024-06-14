package plugin

import (
	"context"
	"errors"
	"github.com/turbot/tailpipe-plugin-sdk/collection"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/source"
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

// GetSchema is the GRPC handler for the GetSchema call
// it builds JSON schemas from parquet tags
// this can be done automatically so there is no need for each plugin to implement this
func (p *Base) GetSchema() (*proto.GetSchemaResponse, error) {
	// TODO implement
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

// TODO define artifact IF?

func (p *Base) NotifyRow(row collection.Row, artifact source.Artifact) error {
	// construct proto event
	e, err := proto.NewRowEvent(row, artifact)
	if err != nil {
		return err
	}

	return p.notifyObservers(e)
}

func (p *Base) NotifyComplete(err error) error {
	// construct proto event
	return p.notifyObservers(proto.NewCompleteEvent(err))
}

func (p *Base) notifyObservers(e *proto.Event) error {
	p.observerLock.RLock()
	defer p.observerLock.RUnlock()
	var notifyErrors []error
	for _, observer := range p.Observers {
		observer.Send(e)
	}

	return errors.Join(notifyErrors...)
}

func (p *Base) Init(context.Context) error {
	return nil
}

func (p *Base) Shutdown(context.Context) error {
	return nil
}
