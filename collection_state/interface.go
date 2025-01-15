package collection_state

import (
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"time"
)

type CollectionState[T parse.Config] interface {
	IsEmpty() bool
	Init(config T, path string) error
	Save() error
	SetGranularity(time.Duration)
	ShouldCollect(id string, timestamp time.Time) bool
	OnCollected(id string, timestamp time.Time) error
	GetGranularity() time.Duration
	GetStartTime() time.Time
	GetEndTime() time.Time
	Clear()
	SetEndTime(time.Time)
}

type ArtifactCollectionState[T parse.Config] interface {
	CollectionState[T]
	RegisterPath(path string, metadata map[string]string)
}
