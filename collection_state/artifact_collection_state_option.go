package collection_state

import (
	"time"
)

type ArtifactCollectionStateOption func(*ArtifactCollectionState)

// TODO #paging kai how to call this function - maybe just set manually
func WithGranularity(granularity time.Duration) ArtifactCollectionStateOption {
	return func(s *ArtifactCollectionState) {
		s.granularity = granularity
	}
}
