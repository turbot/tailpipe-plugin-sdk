package paging

import (
	"sync"
)

type PagingBase struct {
	Mut sync.RWMutex
}

func (b *PagingBase) GetMut() *sync.RWMutex {
	return &b.Mut
}
