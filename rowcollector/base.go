package rowcollector

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"sync"
)

// Base should be embedded in all plugins, collections and sources
type Base struct {
	events.Observable

	// row buffer keyed by execution id
	// each row buffer is used to write a JSONL file
	rowBufferMap map[string][]any
	// mutex for row buffer map AND rowCountMap
	rowBufferLock sync.RWMutex

	// map of row counts keyed by execution id
	rowCountMap map[string]int
}

func (p *Base) Init(context.Context) error {
	p.rowBufferMap = make(map[string][]any)
	p.rowCountMap = make(map[string]int)
	return nil
}

func (p *Base) Shutdown(context.Context) error {
	return nil
}
