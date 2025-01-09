package collection_state

import (
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"sync"
	"time"
)

type CollectionState[T parse.Config] interface {
	GetMut() *sync.RWMutex
	IsEmpty() bool
	Init(config T) error
	GetStartTime() time.Time
	GetEndTime() time.Time
	SetEndTime(time.Time)
}

type ArtifactCollectionState[T parse.Config] interface {
	CollectionState[T]
	ShouldCollect(*types.ArtifactInfo) bool
	OnCollected(*types.ArtifactInfo) error
	GetGranularity() time.Duration
}
