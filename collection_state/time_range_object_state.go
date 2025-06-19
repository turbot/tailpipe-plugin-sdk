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
// timeRangeObjectState is a struct that tracks time ranges and objects that have been collected
// it is used by TimeRangeSliceCollectionState
// NOTE: we do not implement mutex locking here - it is assumed that the caller will lock the state before calling
// NOTE: this struct DOES NOT implement the CollectionState interface directly
type timeRangeObjectState struct {
	// the start time of the range
	From time.Time `json:"from,omitzero"`
	// the end time of the range - we have all data up to this time (non-inclusive)
	// so - if the granularity is 1 hour, and the end time is 12:00:00, we have all data up to 11:59:59
	To time.Time `json:"to,omitzero"`

	// for upper boundary (i.e. the end granularity) we store the metadata
	// whenever the upper boundary time changes, we must clear the map
	// NOTE: for forwards collection, the end objects are at the To time
	// for backwards collection, the end objects are at the From time
	EndObjects map[string]struct{} `json:"end_objects"`

	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	Granularity time.Duration `json:"granularity"`

	// are we collecting forwards (the default) or backwards
	CollectionOrder CollectionOrder `json:"collection_order"`
}

func newTimeRangeCollectionState(from time.Time, order CollectionOrder) *timeRangeObjectState {
	return &timeRangeObjectState{
		From: from,
		// initially the end time is the same as the start time, i.e. we are empty
		To:         from,
		EndObjects: make(map[string]struct{}),
		// default granularity is 1 nanosecond - the default for api sources
		// this will be overridden by ArtifactCollectionState as needed
		Granularity:     1 * time.Nanosecond,
		CollectionOrder: order,
	}
}

func (s *timeRangeObjectState) IsEmpty() bool {
	return s.To.Equal(s.From) || (s.From.IsZero() || s.To.IsZero()) && len(s.EndObjects) == 0
}

// ShouldCollect returns whether the object should be collected
func (s *timeRangeObjectState) ShouldCollect(id string, timestamp time.Time) bool {
	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we use start objects to track everything
	if s.Granularity == 0 {
		// if we do not have a granularity we only use the start map
		return !s.endObjectsContain(id)
	}

	// if the time is between the lowe and upper boundary we should NOT collect
	// (as have already collected it- assuming consistent artifact ordering)
	if s.onOrInsideLowerBoundary(timestamp) && s.insideUpperBoundary(timestamp) {
		return false
	}

	// if the time within a granularity period of the upper boundary time, we must check if we have already collected it
	// (as we have reached the limit of the granularity)
	if timestamp.Sub(s.upperBoundaryTime()) <= s.Granularity {
		return !s.endObjectsContain(id)
	}

	// so it before the current start time or after the current end time - we should collect
	return true
}

// OnCollected is called when an object has been collected - update the end time and end objects if needed
// Note: the object name is the full path to the object
func (s *timeRangeObjectState) OnCollected(id string, timestamp time.Time) error {
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

	// if the timestamp is INSIDE the upper boundary (i.e. before the current END time for chronological),
	// this is unexpected - the upper boundary represents the time which we THOUGHT we had collected all data up to
	// this may indicate that the 'delivery delay'  for the source has been set incorrectly
	// Delivery delay is the maximum lateness in reporting that we expect from a source.
	// Thus if an API may report log entries up[ to 1 hour late, the delivery delay should be set to 1 hour
	// If it is set to 1 hour but then reports an entry 2 hours late, this condition would occur
	switch {
	case s.insideUpperBoundary(timestamp):
		slog.Warn("Artifact timestamp is before the end time, i.e. the time up to which we believed we had collected all data - this may indicate a delay in delivering log lines", "item timestamp", timestamp, "collection state end time", s.To)
		return nil
	case s.outsideUpperBoundary(timestamp):
		// set the upper boundary time to the timestamp
		s.setUpperBoundaryTime(timestamp)
		// clear the end objects map and add the object to the end objects
		s.EndObjects = map[string]struct{}{
			id: struct{}{},
		}
	default:
		// the timestamp must be ON the upper boundary, add object to end objects
		if (s.CollectionOrder == CollectionOrderChronological && timestamp.Equal(s.To)) ||
			(s.CollectionOrder == CollectionOrderReverse && timestamp.Equal(s.From)) {
			s.EndObjects[id] = struct{}{}
		}
	}

	return nil
}

func (s *timeRangeObjectState) GetFromTime() time.Time {
	return s.From
}

// GetToTime returns the time we know have collected ALL data up until
// (we may have collected some data after this - within the granularity period
func (s *timeRangeObjectState) GetToTime() time.Time {
	// i.e. the last time period we are sure we have ALL data for
	return s.To
}

// SetGranularity sets the granularity of the collection state - this is determined by the file layout and the
// granularity of the time metadata it contains
func (s *timeRangeObjectState) SetGranularity(granularity time.Duration) {
	s.Granularity = granularity
}

// GetGranularity returns the granularity of the collection state
func (s *timeRangeObjectState) GetGranularity() time.Duration {
	return s.Granularity
}

// Contains returns whether a timestamp fall within the time range?
// this checks if the timestamp lies within the From and to times of the collection state (inclusive)
func (s *timeRangeObjectState) Contains(timestamp time.Time) bool {
	// TODO can this be called if granularity is zero, i.e. there is no time information?? how does calling code handle this???

	// is this timestamp within the time range from the firstEntryTime to the end of the end objects?
	return timestamp.Compare(s.From) >= 0 && timestamp.Compare(s.To) <= 0
}

func (s *timeRangeObjectState) onOrInsideLowerBoundary(timestamp time.Time) bool {
	// this is true if the timestamp is on the lower boundary time, or inside the lower boundary time
	// for chronological collection, this returns whether the time isON or  AFTER the start time
	// for reverse collection, this returns whether the time is ON or BEFORE the end time
	return s.lowerBoundaryTime().Equal(timestamp) || s.insideLowerBoundary(timestamp)
}

// insideLowerBoundary returns whether the timestamp is inside the lower boundary time (exclusive, i.e. NOT including the boundary time itself)
// for chronological collection, this returns whether the time is AFTER the start time
// for reverse collection, this returns whether the time is BEFORE the end time
func (s *timeRangeObjectState) insideLowerBoundary(timestamp time.Time) bool {

	if s.CollectionOrder == CollectionOrderChronological {
		return timestamp.After(s.From)
	}
	return timestamp.Before(s.To)
}

// insideUpperBoundary returns whether the timestamp is inside the upper boundary time (exclusive, i.e. NOT including the boundary time itself)
// for chronological collection, this returns whether the time is BEFORE the end time
// for reverse collection, this returns whether the time is AFTER the start time
func (s *timeRangeObjectState) insideUpperBoundary(timestamp time.Time) bool {
	if s.CollectionOrder == CollectionOrderChronological {
		return timestamp.Before(s.To)
	}
	return timestamp.After(s.From)
}

// outsideLowerBoundary returns whether the timestamp is outside the lower boundary time
// for chronological collection, this returns whether the time is BEFORE the start time
// for reverse collection, this returns whether the time is AFTER the end time
func (s *timeRangeObjectState) outsideLowerBoundary(timestamp time.Time) bool {
	if s.CollectionOrder == CollectionOrderChronological {
		return timestamp.Before(s.From)
	}
	return timestamp.After(s.To)
}

// outsideUpperBoundary returns whether the timestamp is outside the upper boundary time
// for chronological collection, this returns whether the time is AFTER the end time
// for reverse collection, this returns whether the time is BEFORE the start time
func (s *timeRangeObjectState) outsideUpperBoundary(timestamp time.Time) bool {
	if s.CollectionOrder == CollectionOrderChronological {
		return timestamp.After(s.To)
	}
	return timestamp.Before(s.From)
}

// upperBoundaryTime returns the the furthest time in the direction of collection
// i.e. if we are collecting forwards, the upperBoundaryTime is the end time of the range,
// if we are collecting backwards, the upperBoundaryTime is the start time of the range
func (s *timeRangeObjectState) upperBoundaryTime() time.Time {
	if s.CollectionOrder == CollectionOrderChronological {
		return s.To
	}
	return s.From
}

// lowerBoundaryTime returns the the furthest time in the opposite direction of collection
// i.e. if we are collecting forwards, the lowerBoundaryTime is the start time of the range,
// if we are collecting backwards, the lowerBoundaryTime is the end time of the range
func (s *timeRangeObjectState) lowerBoundaryTime() time.Time {
	if s.CollectionOrder == CollectionOrderChronological {
		return s.From
	}
	return s.To
}

// setUpperBoundaryTime sets the 'to' time for the collection state. This is called:
// - when an object is collected with a timestamp that is after the current 'to' time
// - at the end of a successful collection to indicate that we have collected up to the collection 'to' time
func (s *timeRangeObjectState) setUpperBoundaryTime(newTime time.Time) {

	// truncate the time to the granularity (this will be necessary if the end time is the now-time of a collection)
	newTime = newTime.Truncate(s.Granularity)

	// if timestamp is inside current re, do nothing (?) - this function expected end time to always move forwards
	//  TODO - think about how we clear state in case of explicit recollection - add explicit Clear(fro, to) method?
	if s.insideUpperBoundary(newTime) {
		slog.Debug("setUpperBoundaryTime called with a time that is before or equal To the current end time - ignoring", "new end time", newTime, "current end time", s.To)
		return
	}

	if s.CollectionOrder == CollectionOrderChronological {
		// set the new end time
		s.To = newTime
	} else {
		// for reverse collection, we set the from time to the new end time
		s.From = newTime
	}
	// clear the end objects
	s.EndObjects = make(map[string]struct{})
}

// merge combines this time range with another time range
// note - it is expected that the calling code has determined whether the two ranges should be merged
// - we do not check that here
// important to note that the states bing merged MAY NOT be contiguous
// - as we merge all states between collection From and to on successful completion
func (s *timeRangeObjectState) merge(other *timeRangeObjectState) {
	if s == nil || other == nil {
		return
	}
	// set from and to the the latest of the two
	if other.From.Before(s.From) {
		s.From = other.From
	}
	if other.To.After(s.To) {
		s.To = other.To
		s.EndObjects = other.EndObjects
	}
}

func (s *timeRangeObjectState) endObjectsContain(id string) bool {
	_, ok := s.EndObjects[id]
	return ok
}
