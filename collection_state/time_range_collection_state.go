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

// TODO think about reverse order collection - everything is reversed
// TODO think abolut artifact source with zero granularity - i.e. no time information - how does source handle this???
//
//	do we need a different collection state for this?
//
// timeRangeCollectionState is a struct that tracks time ranges and objects that have been collected
// it is used by the ArtifactCollectionStateImpl and TimeRangeCollectionState
// NOTE: we do not implement mutex locking here - it is assumed that the caller will lock the state before calling
type timeRangeCollectionState struct {
	// the start time of the range
	From time.Time `json:"from,omitzero"`
	// the end time of the range - we have all data up to this time (non-inclusive)
	// so - if the granularity is 1 hour, and the end time is 12:00:00, we have all data up to 11:59:59
	To time.Time `json:"to,omitzero"`

	// for end boundary (i.e. the end granularity) we store the metadata
	// whenever the end time changes, we must clear the map
	EndObjects map[string]struct{} `json:"end_objects"`

	// the granularity of the file naming scheme - so we must keep track of object metadata
	// this will depend on the template used to name the files
	Granularity time.Duration `json:"granularity"`

	// are we collecting forwards (the default) or backwards
	CollectionOrder CollectionOrder `json:"collection_order"`
}

func newTimeRangeCollectionState(from time.Time, order CollectionOrder) *timeRangeCollectionState {
	return &timeRangeCollectionState{
		From: from,
		// initially the end time is the same as the start time, i.e. we are empty
		To:         from,
		EndObjects: make(map[string]struct{}),
		// default granularity is 1 nanosecond - the default for api sources
		// this will be overridden by ArtifactCollectionStateImpl as needed
		Granularity:     1 * time.Nanosecond,
		CollectionOrder: order,
	}
}

func (s *timeRangeCollectionState) IsEmpty() bool {
	return s.To.Equal(s.From) && len(s.EndObjects) == 0
}

// ShouldCollect returns whether the object should be collected
func (s *timeRangeCollectionState) ShouldCollect(id string, timestamp time.Time) bool {
	// if we do not have a granularity set, that means the template does not provide any timing information
	// - we use start objects to track everything
	if s.Granularity == 0 {
		// if we do not have a granularity we only use the start map
		return !s.endObjectsContain(id)
	}

	// if the time is between the start and end time (exclusive) we should NOT collect
	// (as have already collected it- assuming consistent artifact ordering)
	if timestamp.Compare(s.From) >= 0 && timestamp.Compare(s.To) < 0 {
		return false
	}

	// if the timer is <= the end time + granularity, we must check if we have already collected it
	// (as we have reached the limit of the granularity)
	if timestamp.Compare(s.To.Add(s.Granularity)) <= 0 {
		return !s.endObjectsContain(id)
	}

	// so it before the current start time or after the current end time - we should collect
	return true
}

// OnCollected is called when an object has been collected - update the end time and end objects if needed
// Note: the object name is the full path to the object
func (s *timeRangeCollectionState) OnCollected(id string, timestamp time.Time) error {
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

	if s.CollectionOrder == CollectionOrderChronological {
		// if the timestamp is before the current END time, this is unexpected
		// - the end time represents the time which we THOUGHT we had collected all data up to
		// this may indicate that the 'delivery delay'  for the source has been set incorrectly
		// Delivery delay is the maximum lateness in reporting that we expect from a source.
		// Thus if an API may report log entries up[ to 1 hour late, the delivery delay should be set to 1 hour
		// If it is set to 1 hour but then reports an entry 2 hours late, this condition would occur
		if timestamp.Before(s.To) {
			slog.Warn("Artifact timestamp is before the end time, i.e. the time up to which we believed we had collected all data - this may indicate a delay in delivering log lines", "item timestamp", timestamp, "collection state end time", s.To)
			return nil
		}

		// if the timestamp is after the To time, update the To time
		// this may also update the end time
		if timestamp.After(s.To) {
			s.setEndTime(timestamp)
		}
		// if the timestamp is at the To time, add object to end objects
		if timestamp.Equal(s.To) {
			s.EndObjects[id] = struct{}{}
		}

		return nil
	}

	// TODO think about this
	// reverse order collection
	//// if the timestamp is before the current first entry time, just update the first entry time
	//if timestamp.Before(s.firstEntryTime) {
	//	s.firstEntryTime = timestamp
	//}
	//
	//// if the timestamp is at or after after the end time, it must be in the granularity boundary zone
	//// so add to end objects
	//// (this is the same for forwards and backwards collection)
	//if timestamp.Compare(s.To) >= 0 {
	//	s.EndObjects[id] = struct{}{}
	//}

	return nil
}

// setEndTime sets the 'To' time for the collection state. This is called:
// - when an object is collected with a timestamp that is after the current 'To' time
// - at the end of a successful collection to indicate that we have collected up to the collection 'To' time
func (s *timeRangeCollectionState) setEndTime(newEndTime time.Time) {

	// truncate the time to the granularity (this will be necessary if the end time is the now-time of a collection)
	newEndTime = newEndTime.Truncate(s.Granularity)

	// if timestamp is before current To time, do nothing (?) - this function expected end time to always move forwards
	// TODO
	//  - think about how we clear state in case of explcit recolleciton - add explicit Clear(fro, to) method?
	//  - think about reverse order collection - everything is reversed
	if newEndTime.Compare(s.To) <= 0 {
		slog.Debug("setEndTime called with a time that is before or equal to the current end time - ignoring", "new end time", newEndTime, "current end time", s.To)
		return
	}

	// set the new end time
	s.To = newEndTime
	// clear the end objects
	s.EndObjects = make(map[string]struct{})
}

func (s *timeRangeCollectionState) GetFromTime() time.Time {
	return s.From
}

// GetToTime returns the time we know have collected ALL data up until
// (we may have collected some data after this - within the granularity period
func (s *timeRangeCollectionState) GetToTime() time.Time {
	// i.e. the last time period we are sure we have ALL data for
	return s.To
}

// SetGranularity sets the granularity of the collection state - this is determined by the file layout and the
// granularity of the time metadata it contains
func (s *timeRangeCollectionState) SetGranularity(granularity time.Duration) {
	s.Granularity = granularity
}

// GetGranularity returns the granularity of the collection state
func (s *timeRangeCollectionState) GetGranularity() time.Duration {
	return s.Granularity
}

// Contains returns whether a timestamp fall within the time range?
// this checks if the timestamp lies within the from and to times of the collection state (inclusive)
func (s *timeRangeCollectionState) Contains(timestamp time.Time) bool {
	// TODO can this be called if granularity is zero, i.e. there is no time information?? how does calling code handle this???

	// is this timestamp within the time range from the firstEntryTime to the end of the end objects?
	return timestamp.Compare(s.From) >= 0 && timestamp.Compare(s.To) <= 0
}

// merge combines this time range with another time range
// note - it is expected that the calling code has determined whether the two ranges should be merged
// - we do not check that here
// important to note that the states bing merged MAY NOT be contiguous
// - as we merge all states between collection from and to on successful completion
func (s *timeRangeCollectionState) merge(other *timeRangeCollectionState) {
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

func (s *timeRangeCollectionState) endObjectsContain(id string) bool {
	_, ok := s.EndObjects[id]
	return ok
}
