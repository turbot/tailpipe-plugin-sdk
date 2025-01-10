package collection_state

import (
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"sync"
	"time"
)

type SourceItemMetadata interface {
	GetTimestamp() time.Time
	Identifier() string
}

type CollectionState[T parse.Config] interface {
	GetMut() *sync.RWMutex
	IsEmpty() bool
	Init(config T, path string) error
	Save() error
	SetJSONPath(path string)
	GetStartTime() time.Time
	GetEndTime() time.Time
	SetEndTime(time.Time)
	ShouldCollect(SourceItemMetadata) bool
	OnCollected(SourceItemMetadata) error
	GetGranularity() time.Duration
}
