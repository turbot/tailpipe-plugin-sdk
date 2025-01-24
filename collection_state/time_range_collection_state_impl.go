package collection_state

import (
	"encoding/json"
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
	startTime     time.Time
	lastEntryTime time.Time
	// the time we are sure we have collected all data up to - this is (LastEntryTime - granularity)
	endTime time.Time

	// for end boundary (i.e. the end granularity) we store the metadata
	// whenever the end time changes, we must clear the map
	EndObjects map[string]struct{} `json:"end_objects"`

	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	Granularity time.Duration `json:"granularity,omitempty"`
}

func NewTimeRangeCollectionStateImpl() *TimeRangeCollectionStateImpl {
	return &TimeRangeCollectionStateImpl{
		EndObjects: make(map[string]struct{}),
		// default granularity is 1 nanosecond - the default for api sources
		// this will be overridden by ArtifactCollectionStateImpl as needed
		Granularity: 1 * time.Nanosecond,
	}
}

func (s *TimeRangeCollectionStateImpl) IsEmpty() bool {
	return s.startTime.IsZero() && len(s.EndObjects) == 0
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
	if timestamp.Compare(s.startTime) >= 0 && timestamp.Compare(s.endTime) <= 0 {
		return false
	}

	// if the timer is <= the end time + granularity, we must check if we have already collected it
	// (as we have reached the limit of the granularity)
	if timestamp.Compare(s.endTime.Add(s.Granularity)) <= 0 {
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
		s.EndObjects[id] = struct{}{}
		return nil
	}

	// if this timestamp is BEFORE the start time, we must be recollecting with an earlier start time
	// - clear collection state
	// NOTE: in future, we will be more intelligent about this this and support multiple time ranges for for now just reset
	if timestamp.Before(s.startTime) {
		s.Clear()
	}

	// if start time is not set, set it now
	if s.startTime.IsZero() {
		s.startTime = timestamp
	}

	// if the timestamp is before the current END time, this is unexpected
	// - the end time represents the time which we THOUGHT we had collected all data up to
	// this may indicate that the 'delivery delay'  for the source has been set incorrectly
	// Delivery delay is the maximum lateness in reporting that we expect from a source.
	// Thus if an API may report log entries up[ to 1 hour late, the delivery delay should be set to 1 hour
	// If it is set to 1 hour but then reports an entry 2 hours late, this condition would occur
	// NOTE: THIS ASSUMES COLLECTION IS IN ORDER
	// TODO IMPLEMENT REVERSE ORDERING
	if timestamp.Before(s.endTime) {
		// TODO implement delivery delay
		slog.Warn("Artifact timestamp is before the end time, i.e. the time up to which we believed we had collected all data - this may indicate a delay in delivering log lines", "item timestamp", timestamp, "collection state end time", s.endTime)
		return nil
	}

	// if the timestamp is after the last entry time, update the last entry time
	// this may also update the end time
	if timestamp.After(s.lastEntryTime) {
		s.setLastEntryTime(timestamp)
	}

	// if the timestamp is after the end time, it must be in the granularity boundary zone
	// so add to end objects
	if timestamp.After(s.endTime) {
		s.EndObjects[id] = struct{}{}
	}

	return nil
}

// setLastEntryTime sets the last entry time. It also updates the end time if needed
// the end time is the time up to which we are sure we have collected all data, i.e. the last entry time - granularity
func (s *TimeRangeCollectionStateImpl) setLastEntryTime(timestamp time.Time) {
	s.lastEntryTime = timestamp

	// sets the end time for the collection state. If the new end time is AFTER the current end time,
	// we update the end time and identifu any objects that are now INSIDE the end time and remove from the end objects map

	// NOTE: the end time is <granularity> less than the last entry time
	// i.e if the granularity is 1 hour, and the artifact time is 12:00:00,
	// we are sure we have collected ALL data up to 11:00 so the end time will be 11:00:00,
	newEndTime := timestamp.Add(-s.Granularity)

	switch {
	case newEndTime.Equal(s.endTime):
		// no change
		return
	default:
		// set the end time
		s.endTime = newEndTime
		// just clear the end objects
		s.EndObjects = make(map[string]struct{})
	}
}

// SetEndTime sets the end time for the collection state
func (s *TimeRangeCollectionStateImpl) SetEndTime(newEndTime time.Time) {
	// if we have zero granularity, do not set end time as we do not have timing information
	// (this is not expected)
	if s.Granularity == 0 {
		return
	}

	// truncate the time to the granularity
	newEndTime = newEndTime.Truncate(s.Granularity)

	// if the new end time is before the start time, clear the state
	if newEndTime.Before(s.startTime) {
		s.Clear()
		return
	}

	s.endTime = newEndTime
	s.lastEntryTime = newEndTime
	// clear the end objects
	s.EndObjects = make(map[string]struct{})
}

func (s *TimeRangeCollectionStateImpl) GetStartTime() time.Time {
	return s.startTime
}

func (s *TimeRangeCollectionStateImpl) GetEndTime() time.Time {
	// i.e. the last time period we are sure we have ALL data for
	return s.endTime
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
	s.startTime = time.Time{}
	s.lastEntryTime = time.Time{}
	s.endTime = time.Time{}
	// clear the map
	s.EndObjects = make(map[string]struct{})
}

// TODO we do not want to serialise start and end time if they are zero
// until go 1.24 comes out, we manage this by having separate fields to serialise
// https://github.com/turbot/tailpipe-plugin-sdk/issues/84
func (s *TimeRangeCollectionStateImpl) MarshalJSON() ([]byte, error) {
	// Create a temporary struct to hold serialized values
	type Alias TimeRangeCollectionStateImpl
	temp := struct {
		*Alias
		SerialisedStartTime     *time.Time `json:"startTime,omitempty"`
		SerialisedLastEntryTime *time.Time `json:"lastEntryTime,omitempty"`
		SerialisedEndTime       *time.Time `json:"endTime,omitempty"`
	}{
		Alias: (*Alias)(s),
	}

	// Set serialized values conditionally
	if !s.startTime.IsZero() {
		temp.SerialisedStartTime = &s.startTime
	}
	if !s.lastEntryTime.IsZero() {
		temp.SerialisedLastEntryTime = &s.lastEntryTime
	}
	if !s.endTime.IsZero() {
		temp.SerialisedEndTime = &s.endTime
	}

	return json.Marshal(temp)
}

// UnmarshalJSON override unmashal to handle the special case of the start and end time
func (s *TimeRangeCollectionStateImpl) UnmarshalJSON(data []byte) error {
	// Create a temporary struct to hold serialized values
	type Tmp struct {
		// the time range of the data
		// the time of the earliest entry in the data
		StartTime     time.Time `json:"startTime,omitempty"`
		LastEntryTime time.Time `json:"lastEntryTime,omitempty"`
		// the time we are sure we have collected all data up to - this is (LastEntryTime - granularity)
		EndTime time.Time `json:"endTime,omitempty"`

		// for end boundary (i.e. the end granularity) we store the metadata
		// whenever the end time changes, we must clear the map
		EndObjects map[string]struct{} `json:"end_objects,omitempty"`

		// the granularity of the file naming scheme - so we must keep track of object metadata
		// this will depend on the template used to name the files
		Granularity time.Duration `json:"granularity,omitempty"`
	}

	var dest Tmp

	// Unmarshal into the temporary struct
	err := json.Unmarshal(data, &dest)
	if err != nil {
		return err
	}

	// Set the values from the temporary struct
	s.startTime = dest.StartTime
	s.lastEntryTime = dest.LastEntryTime
	s.endTime = dest.EndTime
	s.EndObjects = dest.EndObjects
	// ensure the map is not nil
	if s.EndObjects == nil {
		s.EndObjects = make(map[string]struct{})
	}
	s.Granularity = dest.Granularity
	return nil
}
