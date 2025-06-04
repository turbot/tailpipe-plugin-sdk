package collection_state

import (
	"log/slog"
	"time"
)

type TimeRangeSliceCollectionState struct {
	TimeRanges  []*TimeRangeCollectionStateImpl `json:"TimeRanges"`
	Granularity time.Duration                   `json:"granularity"`
	Order       CollectionOrder                 `json:"order"`
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

//func (t *TimeRangeSliceCollectionState) ShouldCollect(id string, timestamp time.Time) bool {
//	if rangeForTime := t.rangeForTime(timestamp); rangeForTime != nil {
//		return rangeForTime.ShouldCollect(id, timestamp)
//	}
//	// this time does not fall within any of the time ranges - we should collect
//	return true
//}

//func (t *TimeRangeSliceCollectionState) OnCollected(id string, timestamp time.Time) error {
//	if rangeForTime := t.rangeForTime(timestamp); rangeForTime != nil {
//		err := rangeForTime.OnCollected(id, timestamp)
//		if err != nil {
//			return err
//		}
//	} else {
//		// this time does not fall within any of the time ranges - create a new range
//		t.addRange(timestamp)
//	}
//	// now determine whether any of our ranges can be merged
//	t.compact()
//	return nil
//}

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

// rangeForTime returns the index of the time range that contains the given timestamp
// if no existing range contains the timestamp, a new range is created and added into the list and its index is returned
func (t *TimeRangeSliceCollectionState) rangeForTime(timestamp time.Time) *TimeRangeCollectionStateImpl {
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

func (t *TimeRangeSliceCollectionState) addRange(timestamp time.Time) *TimeRangeCollectionStateImpl {
	// create a new time range
	newRange := NewTimeRangeCollectionStateImpl(t.Order)
	newRange.SetGranularity(t.Granularity)
	// find the appropriate location to insert the new range into our list
	for i, r := range t.TimeRanges {
		if r.GetStartTime().After(timestamp) {
			// insert the new range before this one
			t.TimeRanges = append(t.TimeRanges[:i], append([]*TimeRangeCollectionStateImpl{newRange}, t.TimeRanges[i:]...)...)
			return newRange
		}
	}
	// if we get here, the new range should be added to the end of the list
	t.TimeRanges = append(t.TimeRanges, newRange)

	return newRange
}

// TODO think about optimising - decorate TimeRangeCollectionStateImpl in linked list???
func (t *TimeRangeSliceCollectionState) GetNextRange(timeRange *TimeRangeCollectionStateImpl) *TimeRangeCollectionStateImpl {
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
