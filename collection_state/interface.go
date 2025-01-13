package collection_state

import (
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"time"
)

type SourceItemMetadata interface {
	GetTimestamp() time.Time
	Identifier() string
}

// TODO KAI ADD ARTIFACT IF
type CollectionState[T parse.Config] interface {
	IsEmpty() bool
	Init(config T, path string) error
	Save() error
	SetGranularity(time.Duration)
	//SetJSONPath(path string)
	//GetStartTime() time.Time
	//GetEndTime() time.Time
	//SetEndTime(time.Time)
	ShouldCollect(SourceItemMetadata) bool
	OnCollected(SourceItemMetadata) error
	GetGranularity() time.Duration
	RegisterPath(path string, metadata map[string]string)
}
