package collection_state

import (
	"sync"
	"time"
)

type CollectionStateBase struct {
	Mut sync.RWMutex `json:"-"`
	// the time rage of the data in the bucket
	StartTime time.Time `json:"start_time,omitempty"`
	EndTime   time.Time `json:"end_time,omitempty"`
}

func (b *CollectionStateBase) GetMut() *sync.RWMutex {
	return &b.Mut
}

func (b *CollectionStateBase) IsEmpty() bool {
	panic("IsEmpty() must be implemented by type embedding CollectionStateBase")
}

func (b *CollectionStateBase) GetStartTime() time.Time {
	return b.StartTime
}

func (b *CollectionStateBase) GetEndTime() time.Time {
	// i.e. the last time period we are sure we have ALL data for
	return b.EndTime
}

func (b *CollectionStateBase) SetEndTime(t time.Time) {
	b.EndTime = t
}
