package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// TODO we need validate the rows types provided by the plugin to ensure they are valid
/*
GetConnection() string
	GetYear() int
	GetMonth() int
	GetDay() int
	GetTpID() string
	GetTpTimestamp() int64
*/

// how may rows to write in each JSONL file
const JSONLChunkSize = 1000

// Base should be embedded in all tailpipe plugin implementations
type Base struct {
	observerLock sync.RWMutex
	// Observers is a list of all Observers that are currently connected
	// for now these are just the GRPC stream corresponding to the AddObserver call
	Observers []EventStream

	// row buffer keyed by execution id
	// each row buffer is used to write a JSONL file
	rowBufferMap map[string][]any
	// mutex for row buffer
	rowBufferLock sync.RWMutex

	// map of chunk indexes keyed by execution id
	// each chunk index represents a JSON
	chunkIndexMap map[string]int
	// mutex for chunk index
	chunkIndexLock sync.RWMutex
}

//// GetSchema is the GRPC handler for the GetSchema call
//// it builds JSON schemas from parquet tags
//// this can be done automatically so there is no need for each plugin to implement this
//func (p *Base) GetSchema() (*proto.GetSchemaResponse, error) {
//	// TODO implement
//	return nil, nil
//}

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

func (p *Base) NotifyStarted() error {
	// construct proto event
	return p.notifyObservers(proto.NewStartedEvent())
}

func (p *Base) OnRow(row any, req *proto.CollectRequest) error {
	if p.rowBufferMap == nil {
		// this musty mean the plugin has overridden the Init function and not called the base
		return errors.New("Base.Init must be called from the plugin Init function")
	}

	// add row to row buffer
	p.rowBufferLock.Lock()
	p.rowBufferMap[req.ExecutionId] = append(p.rowBufferMap[req.ExecutionId], row)
	var rowsToWrite []any
	if len(p.rowBufferMap[req.ExecutionId]) == JSONLChunkSize {
		rowsToWrite = p.rowBufferMap[req.ExecutionId]
		p.rowBufferMap[req.ExecutionId] = nil
		p.rowBufferLock.Unlock()
	}
	p.rowBufferLock.Unlock()

	if len(rowsToWrite) > 0 {
		// convert row to a JSONL file
		return p.writeJSONL(rowsToWrite, req)
	}
	return nil
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
	p.rowBufferMap = make(map[string][]any)
	p.chunkIndexMap = make(map[string]int)
	return nil
}

func (p *Base) Shutdown(context.Context) error {
	return nil
}

func (p *Base) writeJSONL(rows []any, req *proto.CollectRequest) error {
	executionId := req.ExecutionId
	destPath := req.SourceFilePath
	// increment the chunk index
	p.chunkIndexLock.Lock()
	chunkIndex := p.chunkIndexMap[executionId] + 1
	p.chunkIndexMap[executionId] = chunkIndex
	p.chunkIndexLock.Unlock()

	// generate the filename
	filename := filepath.Join(destPath, fmt.Sprintf("%s-%d.jsonl", executionId, chunkIndex))

	// Open the file for writing
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSONL file %s: %w", filename, err)
	}
	defer file.Close()

	// Create a JSON encoder
	encoder := json.NewEncoder(file)

	// Iterate over the data slice and write each item as a JSON object
	for _, item := range rows {
		err := encoder.Encode(item)
		if err != nil {
			return fmt.Errorf("failed to encode item: %w", err)
		}
	}
	return nil
}
