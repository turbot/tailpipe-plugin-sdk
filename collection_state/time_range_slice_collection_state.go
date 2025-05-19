package collection_state

import "time"

type TimeRangeSliceCollectionState struct {
	timeRanges  []*TimeRangeCollectionStateImpl
	Granularity time.Duration   `json:"granularity"`
	Order       CollectionOrder `json:"order"`
}

func (t *TimeRangeSliceCollectionState) IsEmpty() bool {
	for _, timeRange := range t.timeRanges {
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

func (t *TimeRangeSliceCollectionState) ShouldCollect(id string, timestamp time.Time) bool {
	if rangeForTime := t.rangeForTime(timestamp); rangeForTime != nil {
		return rangeForTime.ShouldCollect(id, timestamp)
	}
	// this time does not fall within any of the time ranges - we should collect
	return true
}

func (t *TimeRangeSliceCollectionState) OnCollected(id string, timestamp time.Time) error {
	if rangeForTime := t.rangeForTime(timestamp); rangeForTime != nil {
		err := rangeForTime.OnCollected(id, timestamp)
		if err != nil {
			return err
		}
	} else {
		// this time does not fall within any of the time ranges - create a new range
		t.addRange(timestamp)
	}
	// now determine whether any of our ranges can be merged
	t.mergeRanges()
	return nil
}

func (t *TimeRangeSliceCollectionState) mergeRanges() {
	for i, timeRange := range t.timeRanges {
		// get next range
		if i+1 < len(t.timeRanges) {
			nextRange := t.timeRanges[i+1]
			if timeRange.CanMerge(nextRange) {
				// merge the two ranges
				timeRange.Merge(nextRange)
				// remove the next range from the list
				t.timeRanges = append(t.timeRanges[:i+1], t.timeRanges[i+2:]...)
			}
		}
	}
}

func (t *TimeRangeSliceCollectionState) GetStartTime() time.Time {
	if len(t.timeRanges) == 0 {
		return time.Time{}
	}
	return t.timeRanges[0].GetStartTime()
}

func (t *TimeRangeSliceCollectionState) GetEndTime() time.Time {
	if len(t.timeRanges) == 0 {
		return time.Time{}
	}
	return t.timeRanges[len(t.timeRanges)-1].GetEndTime()
}

func (t *TimeRangeSliceCollectionState) Clear() {
	t.timeRanges = nil
}

func (t *TimeRangeSliceCollectionState) SetEndTime(endTime time.Time) {
	if len(t.timeRanges) > 0 {
		t.timeRanges[len(t.timeRanges)-1].SetEndTime(endTime)
	}
}

func (t *TimeRangeSliceCollectionState) rangeForTime(timestamp time.Time) *TimeRangeCollectionStateImpl {
	for _, r := range t.timeRanges {
		if r.Contains(timestamp) {
			return r
		}
	}
	return nil
}

func (t *TimeRangeSliceCollectionState) addRange(timestamp time.Time) {
	// create a new time range
	newRange := NewTimeRangeCollectionStateImpl(t.Order)
	newRange.SetGranularity(t.Granularity)
	// find the appropriate location to insert the new range into our list
	for i, r := range t.timeRanges {
		if r.GetStartTime().After(timestamp) {
			// insert the new range before this one
			t.timeRanges = append(t.timeRanges[:i], append([]*TimeRangeCollectionStateImpl{newRange}, t.timeRanges[i:]...)...)
			return
		}
	}
	// if we get here, the new range should be added to the end of the list
	t.timeRanges = append(t.timeRanges, newRange)
}
