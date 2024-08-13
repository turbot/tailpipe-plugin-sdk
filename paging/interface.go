package paging

import "sync"

type Data interface {
	GetMut() *sync.RWMutex
}
