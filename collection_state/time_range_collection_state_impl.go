package collection_state

import (
	"log/slog"
	"time"
)

// TimeRangeCollectionStateImpl is a struct that tracks time ranges and objects that have been collected
// it is used by the ArtifactCollectionStateImpl and TimeRangeCollectionState
// NOTE: we do not implement mutex locking here - it is assumed that the caller will lock the state before calling
type TimeRangeCollectionStateImpl struct {
	// the time range of the data
	StartTime time.Time `json:"start_time,omitempty"`
	EndTime   time.Time `json:"end_time,omitempty"`

	// for end boundary (i.e. the end granularity) we store the metadata
	// whenever the end time changes, we must clear the map
	EndObjects map[string]struct{} `json:"end_objects,omitempty"`

	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	Granularity time.Duration `json:"granularity,omitempty"`
}

func NewTimeRangeCollectionStateImpl() *TimeRangeCollectionStateImpl {
	return &TimeRangeCollectionStateImpl{
		EndObjects: make(map[string]struct{}),
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

	// if start time is not set, set it now
	if s.StartTime.IsZero() {
		s.StartTime = timestamp
	}

	// if this timestamp is BEFORE the start time, we must be recollecting with an earlier styart time
	// - clear collection state
	// NOTE: in future, we will be more intelligent about this this and support multiple time ranges for for now just reset
	if timestamp.Before(s.StartTime) {
		s.StartTime = timestamp
		// clear end time - it will be set by the logic below
		s.EndTime = time.Time{}
	}

	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we must collect everything
	if s.Granularity == 0 {
		s.EndObjects[id] = struct{}{}
		return nil
	}

	// if the timestamp is before the CURRENT end time, then there is an issue
	// - the end time represents the time which we THOUGHT we had collected all data up to
	// this may indicate that the granularity for the sourcre has been set incorrectly
	// (as well as representing the granularity of the time we can deduce from the object name, the granularity also
	// represents the maximum lateness in reporting that we expect from a source.
	// Thus if an API may report log entries up[ to 1 hour late, the granularity should be set to 1 hour
	// If it is set to 1 hour but then reports an entry 2 hours late, thids condition would occur
	if timestamp.Before(s.EndTime) {
		// TODO perhaps we should just update the granularity?
		slog.Warn("Artifact timestamp is before the end time, i.e. the time up to which we believed we had collected all data - this may indicate an incorrect granularity setting", "granularity", s.Granularity, "item timestamp", timestamp, "collection state end time", s.EndTime)
		return nil
	}

	// update our end times as needed
	// NOTE: the end time are adjusted by the granularity
	// i.e if the granularity is 1 hour, and the artifact time is 12:00:00,
	// we are sure we have collected ALL data up to 11:00
	// the end time will be 11:00:00,
	endTime := timestamp.Add(-s.Granularity)
	if endTime.After(s.EndTime) || s.EndTime.IsZero() {
		s.SetEndTime(endTime)
	}

	// add the object to the end map
	s.EndObjects[id] = struct{}{}

	return nil
}

// SetEndTime overrides the base implementation to also clear the end objects
func (s *TimeRangeCollectionStateImpl) SetEndTime(t time.Time) {
	s.EndTime = t
	// clear the end map
	s.EndObjects = make(map[string]struct{})
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
