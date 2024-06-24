package collection

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
)

// Base should be embedded in all tailpipe collection implementations
type Base struct {
	observable.Base
}

// Init implements TailpipePlugin. It is called by Serve when the plugin is started
// if the plugin overrides this function it must call the base implementation
func (p *Base) Init(context.Context) error {
	return nil
}

// Shutdown implements TailpipePlugin. It is called by Serve when the plugin exits
func (p *Base) Shutdown(context.Context) error {
	return nil
}

// OnStarted is called by the plugin when it starts processing a collection request
// any observers are notified
func (p *Base) OnStarted(req *proto.CollectRequest) error {
	// construct proto event
	return p.NotifyObservers(events.NewStarted(req.ExecutionId))
}
func (p *Base) OnRow(row any, req *proto.CollectRequest) error {
	// construct proto event
	// TODO
	return p.NotifyObservers(events.NewRow(req.ExecutionId, row))
}

//
//// OnComplete is called by the plugin when it has finished processing a collection request
//// remaining rows are written and any observers are notified
//func (p *Base) OnComplete(req *proto.CollectRequest, rowCount, chunksWritten int, err error) error {
//	//// write any  remaining rows (call OnRow with a nil row)
//	//// NOTE: this returns the row count
//	//rowCount, err := p.OnRow(nil, req)
//	//if err != nil {
//	//	return err
//	//}
//	//
//	//// notify observers of completion
//	//// figure out the number of chunks written, including partial chunks
//	//chunksWritten := int(rowCount / JSONLChunkSize)
//	//if rowCount%JSONLChunkSize > 0 {
//	//	chunksWritten++
//	//}
//
//	return p.NotifyObservers(events.NewCompleted(req.ExecutionId, rowCount, chunksWritten, err))
//}
