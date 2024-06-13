package plugin

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"log"
	"sync"
)

// PluginBase should be embedded in all tailpipe plugin implementations
type PluginBase struct {
	observerLock sync.RWMutex
	// Observers is a list of all Observers that are currently connected
	// for now these are just the GRPC stream corresponding to the AddObserver call
	Observers []EventStream
}

// AddObserver is the GRPC handler for the AddObserver call
func (p *PluginBase) AddObserver(stream proto.TailpipePlugin_AddObserverServer) error {
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

func (p *PluginBase) NotifyObservers(e Event) {
	p.observerLock.RLock()
	defer p.observerLock.RUnlock()
	for _, observer := range p.Observers {
		observer.Send(e.ToProto())
	}

}

func (p *PluginBase) Init(context.Context) error {
	return nil
}

func (p *PluginBase) Shutdown(context.Context) error {
	return nil
}
