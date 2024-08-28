package collection_state

import (
	"sync"
)

type CollectionStateBase struct {
	Mut sync.RWMutex `json:"-"`
}

func (b *CollectionStateBase) GetMut() *sync.RWMutex {
	return &b.Mut
}
