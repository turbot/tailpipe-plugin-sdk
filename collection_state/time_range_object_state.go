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

// TimeRangeObjectState is a struct that tracks time ranges and objects that have been collected
// it is used by TimeRangeCollectionState
// NOTE: we do not implement mutex locking here - it is assumed that the caller will lock the state before calling
// NOTE: this struct DOES NOT implement the CollectionState interface directly
type TimeRangeObjectState struct {
	// the time range for this collection state
	TimeRange DirectionalTimeRange `json:"time_range"`

	// for the end time (i.e. the end granularity period) we store the metadata
	// whenever the upper boundary time changes, we must clear the map
	// NOTE: for forwards collection, the end objects are at the UpperBoundary time
	// for backwards collection, the end objects are at the LowerBoundary time
	EndObjects map[string]struct{} `json:"end_objects"`

	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	Granularity time.Duration `json:"granularity"`
}

func newTimeRangeCollectionState(from time.Time, order CollectionOrder, granularity time.Duration) *TimeRangeObjectState {
	return &TimeRangeObjectState{
		TimeRange: DirectionalTimeRange{
			LowerBoundary:   from,
			UpperBoundary:   from,
			CollectionOrder: order,
		},
		EndObjects:  make(map[string]struct{}),
		Granularity: granularity,
	}
}

func (s *TimeRangeObjectState) IsEmpty() bool {
	return s.TimeRange.UpperBoundary.Equal(s.TimeRange.LowerBoundary) || (s.TimeRange.LowerBoundary.IsZero() || s.TimeRange.UpperBoundary.IsZero()) && len(s.EndObjects) == 0
}

// ShouldCollect returns whether the object should be collected
func (s *TimeRangeObjectState) ShouldCollect(id string, timestamp time.Time) bool {
	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we use start objects to track everything
	if s.Granularity == 0 {
		// if we do not have a granularity we only use the start map
		gotObject := s.endObjectsContain(id)
		slog.Debug("ShouldCollect called with granularity 0 - checking end objects only", "object", id, "timestamp", timestamp, "got object", gotObject, "should collect", !gotObject)
		return !gotObject
	}

	// if the time is between the lowe and upper boundary we should NOT collect
	// (as have already collected it- assuming consistent artifact ordering)
	if s.TimeRange.onOrAfterStart(timestamp) && s.TimeRange.beforeEnd(timestamp) {
		slog.Debug("ShouldCollect called with time inside the current time range - not collecting", "object", id, "timestamp", timestamp)
		return false
	}

	// if the time within a granularity period of the upper boundary time, we must check if we have already collected it
	// (as we have reached the limit of the granularity)
	if timestamp.Sub(s.TimeRange.endTime()) <= s.Granularity {
		gotObject := s.endObjectsContain(id)
		slog.Debug("ShouldCollect called with time within granularity of upper boundary - checking end objects", "object", id, "timestamp", timestamp, "got object", gotObject, "should collect", !gotObject)
		return !gotObject
	}

	slog.Debug("ShouldCollect called with time outside the current time range - collecting", "object", id, "timestamp", timestamp, "current start time", s.TimeRange.LowerBoundary, "current end time", s.TimeRange.UpperBoundary)
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
	case s.TimeRange.beforeEnd(timestamp):
		// if the timestamp is INSIDE the upper boundary we have nothing to do
		// (this may be caused by a concurrent download of a later file completing first)
		break
	case s.TimeRange.afterEnd(timestamp):
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
	return s.TimeRange.LowerBoundary
}

// GetToTime returns the time we know have collected ALL data up until
// (we may have collected some data after this - within the granularity period
func (s *TimeRangeObjectState) GetToTime() time.Time {
	// i.e. the last time period we are sure we have ALL data for
	return s.TimeRange.UpperBoundary
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
	if !s.TimeRange.LowerBoundary.Equal(other.TimeRange.LowerBoundary) {
		return false, fmt.Sprintf("from = %v, want %v", s.TimeRange.LowerBoundary, other.TimeRange.LowerBoundary)
	}
	if !s.TimeRange.UpperBoundary.Equal(other.TimeRange.UpperBoundary) {
		return false, fmt.Sprintf("UpperBoundary = %v, want %v", s.TimeRange.UpperBoundary, other.TimeRange.UpperBoundary)
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

	if s.TimeRange.beforeEnd(newTime) {
		slog.Debug("extendEndTime called with a time that is before or equal UpperBoundary the current end time - ignoring", "new end time", newTime, "current end time", s.TimeRange.UpperBoundary)
		return
	}

	s.TimeRange.extendEndTime(newTime)

	// if the upper boundary time is NOT today, clear the end objects
	// - we know we have collected all data for that time period
	// (if it is today, we may not have colleceted all data for today yet)
	// TODO #CS take delivery delay into account
	if newTime.Sub(time.Now().Truncate(s.Granularity)) != 0 {
		s.EndObjects = make(map[string]struct{})
	}
}

// merge combines this time range with another time range
// note - it is expected that the calling code has determined whether the two ranges should be merged
// - we do not check that here
// important to note that the states bing merged MAY NOT be contiguous
// - as we merge all states between collection LowerBoundary and to on successful completion
func (s *TimeRangeObjectState) merge(other *TimeRangeObjectState) {
	if s == nil || other == nil {
		return
	}
	// set from and to the the latest of the two
	if other.TimeRange.LowerBoundary.Before(s.TimeRange.LowerBoundary) {
		s.TimeRange.LowerBoundary = other.TimeRange.LowerBoundary
	}
	if other.TimeRange.UpperBoundary.After(s.TimeRange.UpperBoundary) {
		s.TimeRange.UpperBoundary = other.TimeRange.UpperBoundary
		s.EndObjects = other.EndObjects
	}
}

func (s *TimeRangeObjectState) endObjectsContain(id string) bool {
	_, ok := s.EndObjects[id]
	return ok
}

func (s *TimeRangeObjectState) Clone() *TimeRangeObjectState {
	res := &TimeRangeObjectState{
		TimeRange:   s.TimeRange,
		EndObjects:  make(map[string]struct{}, len(s.EndObjects)),
		Granularity: s.Granularity,
	}
	for k := range s.EndObjects {
		res.EndObjects[k] = struct{}{}
	}
	return res
}

func (s *TimeRangeObjectState) String() string {
	return fmt.Sprintf("TimeRangeObjectState{LowerBoundary: %s, UpperBoundary: %s, Granularity: %s, EndObjects: %d}",
		s.TimeRange.LowerBoundary.Format(time.RFC3339),
		s.TimeRange.UpperBoundary.Format(time.RFC3339),
		s.Granularity.String(), len(s.EndObjects))

}
