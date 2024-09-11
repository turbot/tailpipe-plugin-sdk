package collection_state

import (
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/parse"
)

type CollectionStateRange struct {
	StartTime        time.Time      `json:"start_time,omitempty"`
	EndTime          time.Time      `json:"end_time,omitempty"`
	StartIdentifiers map[string]any `json:"start_identifiers,omitempty"`
	EndIdentifiers   map[string]any `json:"end_identifiers,omitempty"`
}

type GenericCollectionState[T parse.Config] struct {
	CollectionStateBase
	Ranges          []*CollectionStateRange `json:"ranges"`
	HasContinuation bool                    `json:"has_continuation"`
	IsChronological bool                    `json:"is_chronological"`

	currentRange *CollectionStateRange
	mergeRange   *CollectionStateRange

	// TODO: #collectionState - determine if we need granularity
	granularity time.Duration
}

func NewGenericCollectionState[T parse.Config]() CollectionState[T] {
	return &GenericCollectionState[T]{}
}

func (s *GenericCollectionState[T]) Init(config T) error {
	// TODO: Init
	return nil
}

func (s *GenericCollectionState[T]) StartCollection() {
	// short-circuit: if we have no ranges, create a new range for currentRange
	if len(s.Ranges) == 0 {
		s.currentRange = s.addNewRange()
		return
	}

	// If we are reverse-chronological, we need to set mergeRange to latest range and create a new range for currentRange
	if !s.IsChronological {
		s.mergeRange = s.getLatestRange()
		s.currentRange = s.addNewRange()
		return
	}

	if s.HasContinuation {
		// chronological with continuation, currentRange should be the latest range
		s.currentRange = s.getLatestRange()
	} else {
		// chronological without continuation, currentRange should be the earliest range
		s.currentRange = s.getEarliestRange()
	}

	return
}

// EndCollection should be called only in the event of successful completion of a collection
func (s *GenericCollectionState[T]) EndCollection() {
	// short-circuit if we have no mergeRange
	if s.mergeRange == nil {
		return
	}
	// if we have a mergeRange, merge currentRange and mergeRange
	panic("Not Implemented")
}

func (s *GenericCollectionState[T]) IsEmpty() bool {
	return len(s.Ranges) == 0
}

func (s *GenericCollectionState[T]) ShouldCollectRow(ts time.Time, key string) bool {
	for _, r := range s.Ranges {
		// inside an existing range, not a boundary
		if ts.After(r.StartTime) && ts.Before(r.EndTime) {
			return false
		}

		// at the start boundary of an existing range
		if ts.Equal(r.StartTime) {
			// check if we have key in start identifiers
			if _, exists := r.StartIdentifiers[key]; exists {
				return false
			} else {
				return true
			}
		}

		// at the end boundary of an existing range
		if ts.Equal(r.EndTime) {
			// check if we have key in end identifiers
			if _, exists := r.EndIdentifiers[key]; exists {
				return false
			} else {
				return true
			}
		}
	}

	return true
}

func (s *GenericCollectionState[T]) Upsert(ts time.Time, key string, meta any) {
	if meta == nil {
		meta = struct{}{}
	}

	if s.currentRange.StartTime.IsZero() || ts.Before(s.currentRange.StartTime) {
		s.currentRange.StartTime = ts
		s.currentRange.StartIdentifiers = make(map[string]any) // clear start identifiers
		s.currentRange.StartIdentifiers[key] = meta
	}

	if s.currentRange.EndTime.IsZero() || ts.After(s.currentRange.EndTime) {
		s.currentRange.EndTime = ts
		s.currentRange.EndIdentifiers = make(map[string]any) // clear end identifiers
		s.currentRange.EndIdentifiers[key] = meta
	}

	if s.currentRange.StartTime.Equal(ts) {
		s.currentRange.StartIdentifiers[key] = meta
	}

	if s.currentRange.EndTime.Equal(ts) {
		s.currentRange.EndIdentifiers[key] = meta
	}
}

func (s *GenericCollectionState[T]) addNewRange() *CollectionStateRange {
	r := &CollectionStateRange{
		StartIdentifiers: make(map[string]any),
		EndIdentifiers:   make(map[string]any),
	}
	s.Ranges = append(s.Ranges, r)

	return r
}

func (s *GenericCollectionState[T]) getEarliestRange() *CollectionStateRange {
	if len(s.Ranges) == 0 {
		return nil
	}

	if len(s.Ranges) == 1 {
		return s.Ranges[0]
	}

	earliestRange := s.Ranges[0]
	for _, r := range s.Ranges[1:] {
		if r.StartTime.Before(earliestRange.StartTime) {
			earliestRange = r
		}
	}

	return earliestRange
}

func (s *GenericCollectionState[T]) getLatestRange() *CollectionStateRange {
	if len(s.Ranges) == 0 {
		return nil
	}

	if len(s.Ranges) == 1 {
		return s.Ranges[0]
	}

	latestRange := s.Ranges[0]
	for _, r := range s.Ranges[1:] {
		if r.EndTime.After(latestRange.EndTime) {
			latestRange = r
		}
	}

	return latestRange
}
