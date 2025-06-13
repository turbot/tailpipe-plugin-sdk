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
	GetFromTime() time.Time
	// GetToTime returns the time 1 granularity period AFTER the last time we are sure we have collected ALL data for
	// e.g. if end time is 2023-10-10T00:00:00Z and granularity is 1 hour,
	// then we have collected all data up to and including 2023-10-09:23:00:00Z
	GetToTime() time.Time
	OnCollectionStarted(fromTime time.Time, toTime time.Time)
	OnCollectionComplete() error
}

type ArtifactCollectionState[T parse.Config] interface {
	CollectionState[T]
	RegisterPath(path string, metadata map[string]string)
}
