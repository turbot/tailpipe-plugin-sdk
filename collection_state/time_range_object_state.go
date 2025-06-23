package collection_state

import (
	"fmt"
	"log/slog"
	"time"
)

type CollectionOrder int

const (
	CollectionOrderChronological CollectionOrder = iota
	CollectionOrderReverse
)

// TODO think about artifact source with zero granularity - i.e. no time information - how does source handle this???
//
//	do we need a different collection state for this?
//

// TimeRangeObjectState is a struct that tracks time ranges and objects that have been collected
// it is used by TimeRangeCollectionState
// NOTE: we do not implement mutex locking here - it is assumed that the caller will lock the state before calling
// NOTE: this struct DOES NOT implement the CollectionState interface directly
type TimeRangeObjectState struct {
	// the time range for this collection state
	TimeRange CollectionTimeRange `json:"time_range"`

	// for upper boundary (i.e. the end granularity) we store the metadata
	// whenever the upper boundary time changes, we must clear the map
	// NOTE: for forwards collection, the end objects are at the To time
	// for backwards collection, the end objects are at the From time
	EndObjects map[string]struct{} `json:"end_objects"`

	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	Granularity time.Duration `json:"granularity"`
}

func newTimeRangeCollectionState(from time.Time, order CollectionOrder) *TimeRangeObjectState {
	return &TimeRangeObjectState{
		TimeRange: CollectionTimeRange{
			From:            from,
			To:              from,
			CollectionOrder: order,
		},
		EndObjects: make(map[string]struct{}),
		// default granularity is 1 nanosecond - the default for api sources
		// this will be overridden by ArtifactCollectionState as needed
		Granularity: 1 * time.Nanosecond,
	}
}

func (s *TimeRangeObjectState) IsEmpty() bool {
	return s.TimeRange.To.Equal(s.TimeRange.From) || (s.TimeRange.From.IsZero() || s.TimeRange.To.IsZero()) && len(s.EndObjects) == 0
}

// ShouldCollect returns whether the object should be collected
func (s *TimeRangeObjectState) ShouldCollect(id string, timestamp time.Time) bool {
	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we use start objects to track everything
	if s.Granularity == 0 {
		// if we do not have a granularity we only use the start map
		return !s.endObjectsContain(id)
	}

	// if the time is between the lowe and upper boundary we should NOT collect
	// (as have already collected it- assuming consistent artifact ordering)
	if s.TimeRange.onOrInsideLowerBoundary(timestamp) && s.TimeRange.insideUpperBoundary(timestamp) {
		return false
	}

	// if the time within a granularity period of the upper boundary time, we must check if we have already collected it
	// (as we have reached the limit of the granularity)
	if timestamp.Sub(s.TimeRange.upperBoundaryTime()) <= s.Granularity {
		return !s.endObjectsContain(id)
	}

	// so it before the current start time or after the current end time - we should collect
	return true
}

// OnCollected is called when an object has been collected - update the end time and end objects if needed
// Note: the object name is the full path to the object
func (s *TimeRangeObjectState) OnCollected(id string, timestamp time.Time) error {
	// first handle special cases
	// if granularity is zero, that means we have no time information about the object
	// - we cannot store start/end times - just put all objects into the end objects map
	if s.Granularity == 0 {
		// NOTE: if granularity is zero but we ARE passed a timestamp, this is an error
		if !timestamp.IsZero() {
			return fmt.Errorf("OnCollected called with a non-zero timestamp but granularity is zero")
		}
		// store end object and return
		s.EndObjects[id] = struct{}{}
		return nil
	}

	switch {
	case s.TimeRange.insideUpperBoundary(timestamp):
		// if the timestamp is INSIDE the upper boundary we have nothing to do
		// (this may be caused by a concurrent download of a later file completing first)
		break
	case s.TimeRange.outsideUpperBoundary(timestamp):
		// set the upper boundary time to the timestamp
		s.setUpperBoundaryTime(timestamp)
		// clear the end objects map and add the object to the end objects
		s.EndObjects = map[string]struct{}{
			id: struct{}{},
		}
	default:
		// the timestamp must be ON the upper boundary, add object to end objects
		s.EndObjects[id] = struct{}{}
	}

	return nil
}

func (s *TimeRangeObjectState) GetFromTime() time.Time {
	return s.TimeRange.From
}

// GetToTime returns the time we know have collected ALL data up until
// (we may have collected some data after this - within the granularity period
func (s *TimeRangeObjectState) GetToTime() time.Time {
	// i.e. the last time period we are sure we have ALL data for
	return s.TimeRange.To
}

// SetGranularity sets the granularity of the collection state - this is determined by the file layout and the
// granularity of the time metadata it contains
func (s *TimeRangeObjectState) SetGranularity(granularity time.Duration) {
	s.Granularity = granularity
}

// GetGranularity returns the granularity of the collection state
func (s *TimeRangeObjectState) GetGranularity() time.Duration {
	return s.Granularity
}

func (s *TimeRangeObjectState) Validate() error {
	return s.TimeRange.Validate()
}

// Compare compares the current state with another TimeRangeObjectState and returns whether they are equal
// and a message describing any differences
func (s *TimeRangeObjectState) Compare(other *TimeRangeObjectState) (bool, string) {
	if !s.TimeRange.From.Equal(other.TimeRange.From) {
		return false, fmt.Sprintf("from = %v, want %v", s.TimeRange.From, other.TimeRange.From)
	}
	if !s.TimeRange.To.Equal(other.TimeRange.To) {
		return false, fmt.Sprintf("To = %v, want %v", s.TimeRange.To, other.TimeRange.To)
	}
	if len(s.EndObjects) != len(other.EndObjects) {
		return false, fmt.Sprintf("EndObjects length = %v, want %v", len(s.EndObjects), len(other.EndObjects))
	}
	for k := range other.EndObjects {
		if _, ok := s.EndObjects[k]; !ok {
			return false, fmt.Sprintf("EndObjects missing key %v", k)
		}
	}
	if s.Granularity != other.Granularity {
		return false, fmt.Sprintf("Granularity = %v, want %v", s.Granularity, other.Granularity)
	}
	if s.TimeRange.CollectionOrder != other.TimeRange.CollectionOrder {
		return false, fmt.Sprintf("CollectionOrder = %v, want %v", s.TimeRange.CollectionOrder, other.TimeRange.CollectionOrder)
	}
	return true, ""
}

// setUpperBoundaryTime sets the upper boundary time to the new time
// this is called when we have collected all data up to a new time
// NOTE: this clears the end objects map as we are moving to a new time period
func (s *TimeRangeObjectState) setUpperBoundaryTime(newTime time.Time) {
	// truncate the time to the granularity (this will be necessary if the end time is the now-time of a collection)
	newTime = newTime.Truncate(s.Granularity)

	if s.TimeRange.insideUpperBoundary(newTime) {
		slog.Debug("setUpperBoundaryTime called with a time that is before or equal To the current end time - ignoring", "new end time", newTime, "current end time", s.TimeRange.To)
		return
	}

	s.TimeRange.setUpperBoundaryTime(newTime)
	// clear the end objects
	s.EndObjects = make(map[string]struct{})
}

// merge combines this time range with another time range
// note - it is expected that the calling code has determined whether the two ranges should be merged
// - we do not check that here
// important to note that the states bing merged MAY NOT be contiguous
// - as we merge all states between collection From and to on successful completion
func (s *TimeRangeObjectState) merge(other *TimeRangeObjectState) {
	if s == nil || other == nil {
		return
	}
	// set from and to the the latest of the two
	if other.TimeRange.From.Before(s.TimeRange.From) {
		s.TimeRange.From = other.TimeRange.From
	}
	if other.TimeRange.To.After(s.TimeRange.To) {
		s.TimeRange.To = other.TimeRange.To
		s.EndObjects = other.EndObjects
	}
}

func (s *TimeRangeObjectState) endObjectsContain(id string) bool {
	_, ok := s.EndObjects[id]
	return ok
}

// TrimToRemoveOverlap checks if this state overlaps with the given collection range
// and trims the range to remove overlap. Clears end objects if the range is truncated
// or if the upper boundary changes.
func TrimToRemoveOverlap(s *TimeRangeObjectState, timeRange *CollectionTimeRange) []*TimeRangeObjectState {
	stateLower := s.TimeRange.lowerBoundaryTime()
	stateUpper := s.TimeRange.upperBoundaryTime()
	rangeLower := timeRange.lowerBoundaryTime()
	rangeUpper := timeRange.upperBoundaryTime()

	// If no overlap, return the original state unchanged
	if !stateUpper.After(rangeLower) || !stateLower.Before(rangeUpper) {
		copiedState := &TimeRangeObjectState{
			TimeRange:   s.TimeRange,
			EndObjects:  make(map[string]struct{}),
			Granularity: s.Granularity,
		}
		for k := range s.EndObjects {
			copiedState.EndObjects[k] = struct{}{}
		}
		return []*TimeRangeObjectState{copiedState}
	}

	// If the state is completely contained within the removal range, remove it
	if !stateLower.Before(rangeLower) && !stateUpper.After(rangeUpper) {
		return []*TimeRangeObjectState{}
	}

	var result []*TimeRangeObjectState

	// Create state for the part before the removal range
	if stateLower.Before(rangeLower) {
		var beforeFrom, beforeTo time.Time
		if s.TimeRange.CollectionOrder == CollectionOrderReverse {
			// For reverse order: From (later) -> To (earlier)
			beforeFrom = s.TimeRange.From
			beforeTo = rangeLower
		} else {
			// For chronological order: From (earlier) -> To (later)
			beforeFrom = s.TimeRange.From
			beforeTo = rangeLower
		}

		beforeState := &TimeRangeObjectState{
			TimeRange: CollectionTimeRange{
				From:            beforeFrom,
				To:              beforeTo,
				CollectionOrder: s.TimeRange.CollectionOrder,
			},
			EndObjects:  make(map[string]struct{}),
			Granularity: s.Granularity,
		}

		// Only preserve EndObjects if the upper boundary is unchanged
		if s.TimeRange.CollectionOrder == CollectionOrderReverse {
			// For reverse order, upper boundary is From
			if beforeFrom.Equal(s.TimeRange.From) {
				for k := range s.EndObjects {
					beforeState.EndObjects[k] = struct{}{}
				}
			}
		} else {
			// For chronological order, upper boundary is To
			if beforeTo.Equal(s.TimeRange.To) {
				for k := range s.EndObjects {
					beforeState.EndObjects[k] = struct{}{}
				}
			}
		}
		result = append(result, beforeState)
	}

	// Create state for the part after the removal range
	if stateUpper.After(rangeUpper) {
		var afterFrom, afterTo time.Time
		if s.TimeRange.CollectionOrder == CollectionOrderReverse {
			// For reverse order: From (later) -> To (earlier)
			afterFrom = rangeUpper
			afterTo = s.TimeRange.To
		} else {
			// For chronological order: From (earlier) -> To (later)
			afterFrom = rangeUpper
			afterTo = s.TimeRange.To
		}

		afterState := &TimeRangeObjectState{
			TimeRange: CollectionTimeRange{
				From:            afterFrom,
				To:              afterTo,
				CollectionOrder: s.TimeRange.CollectionOrder,
			},
			EndObjects:  make(map[string]struct{}),
			Granularity: s.Granularity,
		}

		// Only preserve EndObjects if the upper boundary is unchanged
		if s.TimeRange.CollectionOrder == CollectionOrderReverse {
			// For reverse order, upper boundary is From
			if afterFrom.Equal(s.TimeRange.From) {
				for k := range s.EndObjects {
					afterState.EndObjects[k] = struct{}{}
				}
			}
		} else {
			// For chronological order, upper boundary is To
			if afterTo.Equal(s.TimeRange.To) {
				for k := range s.EndObjects {
					afterState.EndObjects[k] = struct{}{}
				}
			}
		}
		result = append(result, afterState)
	}

	return result
}

// chooseBoundary returns the correct boundary for From/To based on order and split direction
func (order CollectionOrder) chooseBoundary(origFrom, origTo, boundary time.Time, isLeft bool) time.Time {
	if order == CollectionOrderReverse {
		if isLeft {
			return boundary
		} else {
			return boundary
		}
	}
	return boundary
}
