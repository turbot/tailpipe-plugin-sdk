package collection_state

import (
	"fmt"
	"log/slog"
	"time"
)

type TimeRangeSliceCollectionState struct {
	TimeRanges  []*timeRangeCollectionState `json:"TimeRanges"`
	Granularity time.Duration               `json:"granularity"`
	Order       CollectionOrder             `json:"order"`

	// activeRange represents the time range for the most recent object passed to ShouldCollect
	// we expect ShouldCollect to be called with objects in time order
	// if we join up with the start of the next range, activeRange will be updated to the next range
	// (these ranges will then be merged by compact)
	activeRange *timeRangeCollectionState

	// map of object identifier to collection state which contains the object
	// used to store the range associated with each object between the ShouldCollect call and the OnCollected call
	// (this is required because we do not want to have to recompute the range for each object on every OnCollected call)
	// NOTE: the map entry is cleared after OnCollected is called to minimise memory usage
	objectRangeMap map[string]*timeRangeCollectionState
}

func (t *TimeRangeSliceCollectionState) IsEmpty() bool {
	for _, timeRange := range t.TimeRanges {
		if !timeRange.IsEmpty() {
			return false
		}
	}

	return true
}

func NewTimeRangeSliceCollectionState(order CollectionOrder) *TimeRangeSliceCollectionState {
	return &TimeRangeSliceCollectionState{
		Order: order,
	}
}

func (t *TimeRangeSliceCollectionState) SetGranularity(granularity time.Duration) {
	t.Granularity = granularity
}

func (t *TimeRangeSliceCollectionState) GetGranularity() time.Duration {
	return t.Granularity
}

func (t *TimeRangeSliceCollectionState) GetStartTime() time.Time {
	if len(t.TimeRanges) == 0 {
		return time.Time{}
	}
	return t.TimeRanges[0].GetStartTime()
}

func (t *TimeRangeSliceCollectionState) GetEndTime() time.Time {
	if len(t.TimeRanges) == 0 {
		return time.Time{}
	}
	return t.TimeRanges[len(t.TimeRanges)-1].GetEndTime()
}

func (t *TimeRangeSliceCollectionState) Clear() {
	t.TimeRanges = nil
}

func (t *TimeRangeSliceCollectionState) SetEndTime(endTime time.Time) {
	if len(t.TimeRanges) > 0 {
		t.TimeRanges[len(t.TimeRanges)-1].SetEndTime(endTime)
	}
	// TODO this should merge all ranges which touch start and end time of current collection (how do we know the start time?)
	//  maybe the TimeRangeSliceCollectionState need to store metadata for the current collection? active range, start time, end time, etc.
	// compact
	// todo pass start and end time?
	t.compact()
}

func (t *TimeRangeSliceCollectionState) ShouldCollect(id string, timestamp time.Time) bool {
	// get the active range for this timestamp
	// this will either return the current active range, or create a new one if needed
	// (it also checks whether the current active range has joined with the next range and is so sets the active range to the next range)
	activeRange := t.getActiveRange(timestamp)

	// ask the active range if we should collect
	if !activeRange.ShouldCollect(id, timestamp) {
		return false
	}

	// so we should collect this object - cache the range for this object so that OnCollected can find the
	//  correct range to update
	t.objectRangeMap[id] = activeRange
	return true
}

func (t *TimeRangeSliceCollectionState) OnCollected(id string, timestamp time.Time) error {
	// we should have stored a collection state mapping for this object
	rangeForObject, ok := t.objectRangeMap[id]
	if !ok {
		return fmt.Errorf("no collection state mapping found for item '%s' - this should have been set in ShouldCollect", id)
	}
	// clear the mapping
	delete(t.objectRangeMap, id)

	return rangeForObject.OnCollected(id, timestamp)
}

// getActiveRange returns the currently active time range for the given timestamp
// If there is no active range, it creates a new one for the timestamp.
// If the active range has joined with the next range, it updates the active range to the next range.
func (t *TimeRangeSliceCollectionState) getActiveRange(timestamp time.Time) *timeRangeCollectionState {
	// check whether we currently have an active range, i.e. we have already started collection
	if t.activeRange != nil {
		// check the next range to see if it contains this timestamp - i.e. have we collected up to the next range
		nextRange := t.getNextRange(t.activeRange)
		if nextRange != nil {
			if nextRange.Contains(timestamp) {
				// use the next range as the active
				t.activeRange = nextRange
			}
		}
	} else {
		// we have no active range - this is the beginning of a collection
		// find the range for this timestamp
		// (note - this creates a new range if the timestamp is not contained in any existing range)
		t.activeRange = t.rangeForTime(timestamp)
	}
	return t.activeRange
}

// compact merges adjacent time ranges that can be merged
func (t *TimeRangeSliceCollectionState) compact() {
	slog.Info("Compacting time ranges")
	if len(t.TimeRanges) < 2 {
		// nothing to merge
		return
	}

	for i := len(t.TimeRanges) - 2; i >= 0; i-- {
		if t.TimeRanges[i].CanMerge(t.TimeRanges[i+1]) {
			slog.Info("Merging time ranges", "left range end time", t.TimeRanges[i].GetEndTime(), "right range start time", t.TimeRanges[i+1].GetStartTime())
			t.mergeRangeWithNext(i)
		} else {
			slog.Info("Not merging time ranges", "left range end time", t.TimeRanges[i].GetEndTime(), "right range start time", t.TimeRanges[i+1].GetStartTime())
		}
	}
}

// mergeRangeWithNext merges the time range at index idx with the next time range in the list
func (t *TimeRangeSliceCollectionState) mergeRangeWithNext(idx int) {
	if idx+1 >= len(t.TimeRanges) {
		// no next range
		slog.Warn("No next range to merge with")
		return
	}

	l := t.TimeRanges[idx]
	r := t.TimeRanges[idx+1]

	l.Merge(r)
	// remove r from the list
	t.TimeRanges = append(t.TimeRanges[:idx+1], t.TimeRanges[idx+2:]...)
}

// rangeForTime returns the index of the time range that contains the given timestamp
// if no existing range contains the timestamp, a new range is created and added into the list and its index is returned
func (t *TimeRangeSliceCollectionState) rangeForTime(timestamp time.Time) *timeRangeCollectionState {
	for _, r := range t.TimeRanges {
		if r.Contains(timestamp) {
			slog.Info("Found existing range for time", "timestamp", timestamp, "range firstEntryTime", r.firstEntryTime, "range lastEntryTime", r.lastEntryTime)
			return r
		}
		slog.Info("Range does not contain time", "timestamp", timestamp, "range firstEntryTime", r.firstEntryTime, "range lastEntryTime", r.lastEntryTime)
	}

	// otherwise, add new range
	slog.Info("No existing range for time, calling addRange", "timestamp", timestamp)
	return t.addRange(timestamp)
}

// addRange creates a new time range for the given timestamp and adds it to the collection in the correct position
func (t *TimeRangeSliceCollectionState) addRange(timestamp time.Time) *timeRangeCollectionState {
	// create a new time range
	newRange := newTimeRangeCollectionState(t.Order)
	newRange.SetGranularity(t.Granularity)
	// find the appropriate location to insert the new range into our list
	for i, r := range t.TimeRanges {
		if r.GetStartTime().After(timestamp) {
			// insert the new range before this one
			t.TimeRanges = append(t.TimeRanges[:i], append([]*timeRangeCollectionState{newRange}, t.TimeRanges[i:]...)...)
			return newRange
		}
	}
	// if we get here, the new range should be added to the end of the list
	t.TimeRanges = append(t.TimeRanges, newRange)

	return newRange
}

// getNextRange returns the next time range in the collection after the given timeRange
// TODO think about optimising - decorate timeRangeCollectionState in linked list???
func (t *TimeRangeSliceCollectionState) getNextRange(timeRange *timeRangeCollectionState) *timeRangeCollectionState {
	for i, r := range t.TimeRanges {
		if r == timeRange {
			if i+1 < len(t.TimeRanges) {
				return t.TimeRanges[i+1]
			}
			return nil // no next range
		}
	}
	return nil // timeRange not found in the list
}
