package collection_state

import (
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"time"
)

type SourceItemMetadata interface {
	GetTimestamp() time.Time
	Identifier() string
}

type CollectionState[T parse.Config] interface {
	IsEmpty() bool
	Init(config T, path string) error
	Save() error
	SetGranularity(time.Duration)
	ShouldCollect(SourceItemMetadata) bool
	OnCollected(SourceItemMetadata) error
	GetGranularity() time.Duration
}

type ArtifactCollectionState[T parse.Config] interface {
	CollectionState[T]
	RegisterPath(path string, metadata map[string]string)
}
