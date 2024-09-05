package collection_state

import (
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"sync"
)

type CollectionState[T parse.Config] interface {
	GetMut() *sync.RWMutex
	IsEmpty() bool
	Init(config T) error
}
