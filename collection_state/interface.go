package collection_state

import (
	"time"
)

type CollectionState interface {
	IsEmpty() bool
	Init(*CollectionTimeRange) error
	ShouldCollect(id string, timestamp time.Time) bool
	OnCollected(id string, timestamp time.Time) error
	SetGranularity(time.Duration)
	GetGranularity() time.Duration
	GetFromTime() time.Time
	// GetToTime returns the time 1 granularity period AFTER the last time we are sure we have collected ALL data for
	// e.g. if end time is 2023-10-10T00:00:00Z and granularity is 1 hour,
	// then we have collected all data up to and including 2023-10-09:23:00:00Z
	GetToTime() time.Time
	OnCollectionComplete() error
	MigrateFromLegacyState(bytes []byte) error
	Validate() error
	Clear(CollectionTimeRange)
}

type CollectionStateWithPaths interface {
	CollectionState
	RegisterPath(path string, metadata map[string]string)
}
