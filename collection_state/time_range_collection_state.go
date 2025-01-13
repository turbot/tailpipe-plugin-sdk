package collection_state

import (
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/parse"
)

type CollectionStateTimeRange struct {
	StartTime        time.Time      `json:"start_time,omitempty"`
	EndTime          time.Time      `json:"end_time,omitempty"`
	StartIdentifiers map[string]any `json:"start_identifiers,omitempty"`
	EndIdentifiers   map[string]any `json:"end_identifiers,omitempty"`
}

func NewCollectionStateTimeRange() *CollectionStateTimeRange {
	return &CollectionStateTimeRange{
		StartIdentifiers: make(map[string]any),
		EndIdentifiers:   make(map[string]any),
	}
}

// TimeRangeCollectionState is a collection state that tracks time ranges of collected data from contiguous and non-contiguous sources.
// - Ranges are defined by a start time and an end time. Each range has start and end identifiers to track boundary results.
// - HasContinuation is a boolean that indicates whether the collection has a continuation token or can be continued from DateTime information in Ranges.
// - ContinuationToken is a string that can be used to continue the collection from the last known state.
// - IsChronological is a boolean that indicates whether the collection is chronological or reverse-chronological.
type TimeRangeCollectionState[T parse.Config] struct {
	CollectionStateImpl[T]
	Ranges            []*CollectionStateTimeRange `json:"ranges"`
	HasContinuation   bool                        `json:"has_continuation"`
	ContinuationToken *string                     `json:"continuation_token,omitempty"`
	// TODO: #collectionState - should this be an enum rather than a bool?
	IsChronological bool `json:"is_chronological"`

	currentRange *CollectionStateTimeRange
	mergeRange   *CollectionStateTimeRange
}

func NewTimeRangeCollectionState[T parse.Config]() CollectionState[T] {
	return &TimeRangeCollectionState[T]{}
}

func (s *TimeRangeCollectionState[T]) RegisterPath(path string, metadata map[string]string) {
	// TODO remove need for this - put in artifact IF only?
}

// Init initializes the collection state with the provided configuration
func (s *TimeRangeCollectionState[T]) Init(config T, path string) error {
	return s.CollectionStateImpl.Init(config, path)
}

// StartCollection should be called at the beginning of a collection.
// It sets a currentRange for the collection and optionally a mergeRange based on existing ranges and configuration.
func (s *TimeRangeCollectionState[T]) StartCollection() {
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
}

// EndCollection should be called only in the event of successful completion of a collection
func (s *TimeRangeCollectionState[T]) EndCollection() {
	// merge ranges
	s.Ranges = s.mergeRanges()
}

// IsEmpty returns true if there are no ranges in the collection state
func (s *TimeRangeCollectionState[T]) IsEmpty() bool {
	return len(s.Ranges) == 0
}

// ShouldCollectRow returns true if the row should be collected based on the time and provided key identifier
func (s *TimeRangeCollectionState[T]) ShouldCollectRow(ts time.Time, key string) bool {
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

// Upsert sets or updates the current time range based on the timestamp and key identifier
func (s *TimeRangeCollectionState[T]) Upsert(ts time.Time, key string, meta any) {
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

func (s *TimeRangeCollectionState[T]) addNewRange() *CollectionStateTimeRange {
	r := NewCollectionStateTimeRange()
	s.Ranges = append(s.Ranges, r)

	return r
}

func (s *TimeRangeCollectionState[T]) getEarliestRange() *CollectionStateTimeRange {
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

func (s *TimeRangeCollectionState[T]) getLatestRange() *CollectionStateTimeRange {
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

// GetLatestEndTime returns the end time of the latest range in the collection state
func (s *TimeRangeCollectionState[T]) GetLatestEndTime() *time.Time {
	if len(s.Ranges) == 0 {
		return nil
	}

	return &s.getLatestRange().EndTime
}

// GetEarliestStartTime returns the start time of the earliest range in the collection state
func (s *TimeRangeCollectionState[T]) GetEarliestStartTime() *time.Time {
	if len(s.Ranges) == 0 {
		return nil
	}

	return &s.getEarliestRange().StartTime
}

func (s *TimeRangeCollectionState[T]) mergeRanges() []*CollectionStateTimeRange {
	// short-circuit if we have no mergeRange
	if s.mergeRange == nil {
		return s.Ranges
	}

	// short-circuit if we obtained no new items to currentRange
	if s.currentRange.StartTime.IsZero() || s.currentRange.EndTime.IsZero() {
		return s.Ranges
	}

	// build ranges excluding currentRange and mergeRange
	var existingOtherRanges []*CollectionStateTimeRange
	for _, r := range s.Ranges {
		if r != s.currentRange && r != s.mergeRange {
			existingOtherRanges = append(existingOtherRanges, r)
		}
	}

	mergedRange := NewCollectionStateTimeRange()
	mergedRange.StartTime = s.mergeRange.StartTime
	mergedRange.EndTime = s.currentRange.EndTime
	mergedRange.StartIdentifiers = s.mergeRange.StartIdentifiers
	mergedRange.EndIdentifiers = s.currentRange.EndIdentifiers

	return append(existingOtherRanges, mergedRange)
}

// TODO: #collectionState - do we need GetLatestStartTime / GetEarliestEndTime?
