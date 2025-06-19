package collection_state

import (
	"time"
)

type ReverseOrderCollectionStateLegacy struct {
	// collection of time ranges ordered by time
	TimeRanges []*TimeRangeCollectionStateLegacy `json:"time_ranges"`
}

type TimeRangeCollectionStateLegacy struct {
	FirstEntryTime  time.Time           `json:"first_entry_time,omitempty"`
	LastEntryTime   time.Time           `json:"last_entry_time,omitempty"`
	EndTime         time.Time           `json:"end_time,omitempty"`
	EndObjects      map[string]struct{} `json:"end_objects"`
	Granularity     time.Duration       `json:"granularity,omitempty"`
	CollectionOrder CollectionOrder     `json:"collection_order,omitempty"`
}

type ArtifactCollectionStateLegacy struct {
	TrunkStates      map[string]*TimeRangeCollectionStateLegacy `json:"trunk_states,omitempty"`
	LastModifiedTime time.Time                                  `json:"last_modified_time,omitempty"`
}
