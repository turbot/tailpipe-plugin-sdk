package paging

import (
	"sync"
)

type PagingBase struct {
	mut sync.RWMutex
}

func (b *PagingBase) GetMut() *sync.RWMutex {
	return &b.mut
}
