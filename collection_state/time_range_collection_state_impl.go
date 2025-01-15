package collection_state

import (
	"fmt"
	"log/slog"
	"time"
)

// TimeRangeCollectionStateImpl is a struct that tracks time ranges and objects that have been collected
// it is used by the ArtifactCollectionStateImpl and TimeRangeCollectionState
// NOTE: we do not implement mutex locking here - it is assumed that the caller will lock the state before calling
type TimeRangeCollectionStateImpl struct {
	// the time range of the data
	// the time of the earliest entry in the data
	StartTime     time.Time `json:"first_entry_time,omitempty"`
	LastEntryTime time.Time `json:"last_entry_time,omitempty"`
	// the time we are sure we have collected all data up to - this is (LastEntryTime - granularity)
	EndTime time.Time `json:"end_time,omitempty"`

	// for end boundary (i.e. the end granularity) we store the metadata
	// whenever the end time changes, we must clear the map
	EndObjects map[string]time.Time `json:"end_objects,omitempty"`

	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	Granularity time.Duration `json:"granularity,omitempty"`
}

func NewTimeRangeCollectionStateImpl() *TimeRangeCollectionStateImpl {
	return &TimeRangeCollectionStateImpl{
		EndObjects: make(map[string]time.Time),
		// default granularity is 1 nanosecond - the default for api sources
		// this will be overridden by ArtifactCollectionStateImpl as needed
		Granularity: 1 * time.Nanosecond,
	}
}

func (s *TimeRangeCollectionStateImpl) IsEmpty() bool {
	return s.StartTime.IsZero()
}

// ShouldCollect returns whether the object should be collected
func (s *TimeRangeCollectionStateImpl) ShouldCollect(id string, timestamp time.Time) bool {

	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we use start objects to track everythinbg
	if s.Granularity == 0 {
		// if we do not have a granularity we only use the start map
		return !s.endObjectsContain(id)
	}

	// if the time is between the start and end time (inclusive) we should NOT collect
	// (as have already collected it- assuming consistent artifact ordering)
	if timestamp.Compare(s.StartTime) >= 0 && timestamp.Compare(s.EndTime) <= 0 {
		return false
	}

	// if the timer is <= the end time + granularity, we must check if we have already collected it
	// (as we have reached the limit of the granularity)
	if timestamp.Compare(s.EndTime.Add(s.Granularity)) <= 0 {
		return !s.endObjectsContain(id)
	}

	// so it before the current start time or after the current end time - we should collect
	return true
}

// OnCollected is called when an object has been collected - update the end time and end objects if needed
// Note: the object name is the full path to the object
func (s *TimeRangeCollectionStateImpl) OnCollected(id string, timestamp time.Time) error {
	// first handle special cases
	// if granularity is zero, that means we have no time information about the object
	// - we cannot store start/end times - just put all objects into the end objects map
	if s.Granularity == 0 {
		// NOTE: if granularity is zero but we ARE passed a timestamp, this is an error
		if !timestamp.IsZero() {
			return fmt.Errorf("OnCollected called with a non-zero timestamp but granularity is zero")
		}
		// store end object and return
		s.EndObjects[id] = timestamp
		return nil
	}

	// if this timestamp is BEFORE the start time, we must be recollecting with an earlier start time
	// - clear collection state
	// NOTE: in future, we will be more intelligent about this this and support multiple time ranges for for now just reset
	// TODO THIS MAY NOT BE NEEDED IF SOURCE HANDLES EARLIER 'FROM' TIMES
	if timestamp.Before(s.StartTime) {
		s.StartTime = timestamp
		// clear the last entry time as we have a new start time
		s.setLastEntryTime(timestamp)
		// store the end objects
		s.EndObjects[id] = timestamp
		// and return
		return nil
	}

	// if start time is not set, set it now
	if s.StartTime.IsZero() {
		s.StartTime = timestamp
	}

	// if the timestamp is before the CURRENT end time, then there is an issue
	// - the end time represents the time which we THOUGHT we had collected all data up to
	// this may indicate that the 'delivery delay'  for the source has been set incorrectly
	// Delivery delay is the maximum lateness in reporting that we expect from a source.
	// Thus if an API may report log entries up[ to 1 hour late, the delivery delay should be set to 1 hour
	// If it is set to 1 hour but then reports an entry 2 hours late, this condition would occur
	// NOTE: THIS ASSUMES COLLECTION IS IN ORDER
	// TODO IMPLEMENT REVERSE ORDERING
	if timestamp.Before(s.EndTime) {
		// TODO implement delivery delay
		slog.Warn("Artifact timestamp is before the end time, i.e. the time up to which we believed we had collected all data - this may indicate a delay in delivering log lines", "item timestamp", timestamp, "collection state end time", s.EndTime)
		return nil
	}

	// if the timestamp is after the last entry time, update the last entry time
	// this may also update the end time
	if timestamp.After(s.LastEntryTime) {
		s.setLastEntryTime(timestamp)
	}

	// if the timestamp is after the end time, it must be in the granularity boundary zone
	// so add to end objects
	if timestamp.After(s.EndTime) {
		s.EndObjects[id] = timestamp
	}

	return nil
}

// setLastEntryTime sets the last entry time. It also updates the end time if needed
// the end time is the time up to which we are sure we have collected all data, i.e. the last entry time - granularity
func (s *TimeRangeCollectionStateImpl) setLastEntryTime(timestamp time.Time) {
	s.LastEntryTime = timestamp

	// sets the end time for the collection state. If the new end time is AFTER the current end time,
	// we update the end time and identifu any objects that are now INSIDE the end time and remove from the end objects map
	// NOTE: the end time are adjusted by the granularity
	// i.e if the granularity is 1 hour, and the artifact time is 12:00:00,
	// we are sure we have collected ALL data up to 11:00 so the end time will be 11:00:00,
	newEndTime := timestamp.Add(-s.Granularity)

	switch {
	case newEndTime.Equal(s.EndTime):
		// no change
		return
	case newEndTime.Before(s.EndTime):
		// if the new end time is before the current end time, this must be beacuse a from parameter was passed
		// to force recollection of earlier data - so we must clear the end objects and set the new end time
		// if the new end time is before the start time, just clear the collection state (this should not happen)
		if newEndTime.Before(s.StartTime) {
			s.Clear()
			return
		}

		// set the end time
		s.EndTime = newEndTime
		// clear the end objects
		s.EndObjects = make(map[string]time.Time)
	case newEndTime.After(s.EndTime), s.EndTime.IsZero():
		// if the new end time is after the current end time, update the end time and remove any end objects
		// which are no longer in the boundary zone

		// set the end time
		s.EndTime = newEndTime
		// update the end objects - remove any objects which are now INSIDE the end time
		var endObjectsToDelete []string
		for id, objectTimestamp := range s.EndObjects {
			// if the object is NOT after the new end time, remove it
			if !objectTimestamp.After(newEndTime) {
				endObjectsToDelete = append(endObjectsToDelete, id)
			}
		}
		// clear the objects that are no longer in the end time range
		for _, id := range endObjectsToDelete {
			delete(s.EndObjects, id)
		}
	}
}

// SetEndTime sets the end time for the collection state
func (s *TimeRangeCollectionStateImpl) SetEndTime(newEndTime time.Time) {
	// we actually just set the last entry time adjusting for granularity - this will set the end time for us
	s.setLastEntryTime(newEndTime.Add(s.Granularity))
}

func (s *TimeRangeCollectionStateImpl) GetStartTime() time.Time {
	return s.StartTime
}

func (s *TimeRangeCollectionStateImpl) GetEndTime() time.Time {
	// i.e. the last time period we are sure we have ALL data for
	return s.EndTime
}

// SetGranularity sets the granularity of the collection state - this is determined by the file layout and the
// granularity of the time metadata it contains
func (s *TimeRangeCollectionStateImpl) SetGranularity(granularity time.Duration) {
	s.Granularity = granularity
}

// GetGranularity returns the granularity of the collection state
func (s *TimeRangeCollectionStateImpl) GetGranularity() time.Duration {
	return s.Granularity
}

func (s *TimeRangeCollectionStateImpl) endObjectsContain(id string) bool {
	_, ok := s.EndObjects[id]
	return ok
}

func (s *TimeRangeCollectionStateImpl) Clear() {
	// clear the times
	s.StartTime = time.Time{}
	s.LastEntryTime = time.Time{}
	s.EndTime = time.Time{}
	// clear the map
	s.EndObjects = make(map[string]time.Time)
}
