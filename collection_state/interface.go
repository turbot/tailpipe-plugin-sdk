package collection_state

import "sync"

type CollectionState interface {
	GetMut() *sync.RWMutex
}
