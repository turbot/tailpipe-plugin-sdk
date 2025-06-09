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
	// the active range is extended to cover the timestamp of any object passed to ShouldCollect
	// if the timestamp for an object is contained within a different time range,
	// the active range is set to that time range.
	// NOTE: all ranges within the currentCollectionTimeRange will be merged by the final call to compact
	activeRange *timeRangeCollectionState

	// map of object identifier to collection state which contains the object
	// used to store the range associated with each object between the ShouldCollect call and the OnCollected call
	// (this is required because we do not want to have to recompute the range for each object on every OnCollected call)
	// NOTE: the map entry is cleared after OnCollected is called to minimise memory usage
	objectRangeMap             map[string]*timeRangeCollectionState
	currentCollectionTimeRange *timeRange
}

func (t *TimeRangeSliceCollectionState) IsEmpty() bool {
	for _, timeRange := range t.TimeRanges {
		if !timeRange.IsEmpty() {
			return false
		}
	}

	return true
}

func NewTimeRangeSliceCollectionState(collection *timeRange, order CollectionOrder) *TimeRangeSliceCollectionState {
	res := &TimeRangeSliceCollectionState{
		Order:                      order,
		currentCollectionTimeRange: collection,
		objectRangeMap:             make(map[string]*timeRangeCollectionState),
	}

	// initialise the active range - this will create a new range for the collection From time if needed
	res.updateActiveRange(collection.from)
	return res
}

// Init is called after loading a collection state
// compact the time ranges in case the previous collection was not completed successfully
// (normally the state is compacted before the final save but if the process was killed before that,
// we may have a collection state with multiple ranges that can be merged)
func (t *TimeRangeSliceCollectionState) Init() {
	t.compact()
}

// OnCollectionStarted is called when a new collection is started - set the currentCollectionTimeRange
// and initialise the active range
func (t *TimeRangeSliceCollectionState) OnCollectionStarted(fromTime, toTime time.Time) error {
	// set the start time of the first range to the From time of the collection
	t.currentCollectionTimeRange = &timeRange{
		from: fromTime,
		to:   toTime,
	}

	t.updateActiveRange(t.currentCollectionTimeRange.from)
	return nil

}

// OnCollectionComplete sets the end time of the collect - this is called from OnCollectionCOmplete after a successful collection
// we set the end time as we know that we have collected all data up to the collection 'To' time
// it sets the end time of the active range to the given end time
func (t *TimeRangeSliceCollectionState) OnCollectionComplete() error {
	if t.currentCollectionTimeRange == nil {
		return fmt.Errorf("cannot complete collection - no current collection set, OnCollectionStarted must be called first")
	}

	t.activeRange.setEndTime(t.currentCollectionTimeRange.to)

	// compact
	t.compact()
	return nil
}

func (t *TimeRangeSliceCollectionState) SetGranularity(granularity time.Duration) {
	t.Granularity = granularity
	// set the granularity for all existing time ranges
	// TODO it would be nice to set the granularity when we create the state - see if we can do this
	for _, timeRange := range t.TimeRanges {
		timeRange.SetGranularity(granularity)
	}
}

func (t *TimeRangeSliceCollectionState) GetGranularity() time.Duration {
	return t.Granularity
}

func (t *TimeRangeSliceCollectionState) GetFromTime() time.Time {
	if len(t.TimeRanges) == 0 {
		return time.Time{}
	}
	return t.TimeRanges[0].GetFromTime()
}

func (t *TimeRangeSliceCollectionState) GetToTime() time.Time {
	if len(t.TimeRanges) == 0 {
		return time.Time{}
	}
	return t.TimeRanges[len(t.TimeRanges)-1].GetToTime()
}

func (t *TimeRangeSliceCollectionState) ShouldCollect(id string, timestamp time.Time) bool {
	// get the active range for this timestamp
	// this will either return the current active range, or create a new one if needed
	// (it also checks whether the current active range has joined with the next range and is so sets the active range to the next range)
	t.updateActiveRange(timestamp)

	// ask the active range if we should collect
	if !t.activeRange.ShouldCollect(id, timestamp) {
		return false
	}

	// so we should collect this object - cache the range for this object so that OnCollected can find the
	//  correct range to update
	t.objectRangeMap[id] = t.activeRange
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

// updateActiveRange returns the currently active time range for the given timestamp
// If there is no active range, it creates a new one for the timestamp.
// If the active range has joined with the next range, it updates the active range to the next range.
func (t *TimeRangeSliceCollectionState) updateActiveRange(timestamp time.Time) {
	// check whether we currently have an active range, i.e. we have already started collection
	if t.activeRange != nil {
		// check the all subsequent ranges to see if they contains this timestamp
		// TODO maybe do not worry about ordering ranges - just assume after compact they will be correct
		// TODO ensure always compact before save
		// e.g.
		//
		// #1 we may have no data from 1-5th, a range on 2nd and 4th, now we're collecting 1st-5th so active range starts at 1st
		// but first data is 5th
		//
		// | 1st (active) | 2nd  (empty range) | 3rd | 4th (empty range) | 5th |
		//
		// we just extend initial range to 5th and compact will handle
		//
		// #2 we may have no data from 1-3rd, a range on 2nd and 4th, now we're collecting 1st-5th so active range starts at 1st
		// (first data is 4th)
		//
		// | 1st (active) | 2nd  (empty range) | 3rd | 4th (range with data) | 5th |
		//
		// updateActiveRange should search forward and find that timestamp 4th is within that range and set it to active range
		// compact should merge ALL ranges from colection from-to time, so it will merge the 1st, 2nd and 4th ranges into one
		///
		// search forwards until either:
		// 1. we find a range that contains the timestamp - that is the new active range. The compaction will merge all ranges
		// 2. we reach the end of the list of time ranges - in which case we use the existing active range
		// 3. we find a range that starts after the timestamp - in which case we use the existing active range
		// TODO just use rangeForTime here!
		//  if so we need to change rangeForTime to not aleways create a new range, if none found - either accept options or just make calling code create
		nextRange := t.getNextRange(t.activeRange)
		if nextRange != nil {
			if nextRange.Contains(timestamp) {
				// set the end time of the active range to the start time of the next range so we merge them next time we
				// compact the state
				t.activeRange.setEndTime(nextRange.GetFromTime())
				// use the next range as the active
				t.activeRange = nextRange
			}
		}
	} else {
		// we have no active range - this is the beginning of a collection
		// find the range for this timestamp
		// NOTE: there may already be ranges in the state - as we may  have loaded an existing
		// (note - this creates a new range if the timestamp is not contained in any existing range)
		t.activeRange = t.rangeForTime(timestamp)
	}
}

// compact merges adjacent time ranges that can be merged
func (t *TimeRangeSliceCollectionState) compact() {
	// currentCollectionTimeRange may not be set yet if this is  being called from Init

	// if there are less than 2 time ranges, we cannot compact
	if len(t.TimeRanges) < 2 {
		return
	}

	slog.Info("Compacting time ranges")
	var compactedRanges []*timeRangeCollectionState
	currentRange := t.TimeRanges[0]

	for i := 1; i < len(t.TimeRanges); i++ {
		nextRange := t.TimeRanges[i]

		// we will merge the ranges if EITHER:
		//  - if the ranges overlap, OR
		//  - if the ranges fall within the collection period (i.e. the collection From time is before the end time of currentRange

		rangesOverlap := currentRange.GetToTime().Compare(nextRange.GetFromTime()) >= 0

		rangesFallWithinCollectionPeriod := t.currentCollectionTimeRange != nil &&
			currentRange.GetToTime().After(t.currentCollectionTimeRange.from) &&
			nextRange.GetFromTime().Before(t.currentCollectionTimeRange.to)

		slog.Debug("Checking time ranges for merging",
			"collection from", t.currentCollectionTimeRange.from,
			"collection to", t.currentCollectionTimeRange.to,
			"current range start time", currentRange.GetFromTime(),
			"current range end time", currentRange.GetToTime(),
			"next range start time", nextRange.GetFromTime(),
			"next range end time", nextRange.GetToTime(),
			"ranges overlap", rangesOverlap,
			"ranges fall within collection period", rangesFallWithinCollectionPeriod)

		// if the ranges do not overlap and do not fall within the collection period, we can add the current range to compacted ranges
		if !rangesOverlap && !rangesFallWithinCollectionPeriod {
			// add the current range to compacted ranges and move to next
			compactedRanges = append(compactedRanges, currentRange)
			currentRange = nextRange
			continue
		}

		// tell currentRange to merge with nextRange
		currentRange.merge(nextRange)
		slog.Info("Merging time ranges",
			"left range end time", currentRange.GetToTime(),
			"right range start time", nextRange.GetFromTime(),
			"right range end time", nextRange.GetToTime(),
			"merged range end time", currentRange.GetToTime())
	}

	// add the last range
	compactedRanges = append(compactedRanges, currentRange)

	// now update the TimeRanges slice with the compacted ranges
	if len(compactedRanges) == 0 {
		slog.Info("No time ranges could be merged")
		return
	}
	t.TimeRanges = compactedRanges
	return
}

// rangeForTime returns the index of the time range that contains the given timestamp
// if no existing range contains the timestamp, a new range is created and added into the list and its index is returned
func (t *TimeRangeSliceCollectionState) rangeForTime(timestamp time.Time) *timeRangeCollectionState {
	for _, r := range t.TimeRanges {
		if r.Contains(timestamp) {
			slog.Info("Found existing range for time", "timestamp", timestamp, "range From", r.From, "range TO", r.To)
			return r
		}
		slog.Info("Range does not contain time", "timestamp", timestamp, "range From", r.From, "range TO", r.To)
	}

	// otherwise, add new range
	slog.Info("No existing range for time, calling addRange", "timestamp", timestamp)
	return t.addRange(timestamp)
}

// addRange creates a new time range for the given timestamp and adds it to the collection in the correct position
func (t *TimeRangeSliceCollectionState) addRange(timestamp time.Time) *timeRangeCollectionState {
	// create a new time range
	newRange := newTimeRangeCollectionState(timestamp, t.Order)
	newRange.SetGranularity(t.Granularity)
	// find the appropriate location to insert the new range into our list
	for i, r := range t.TimeRanges {
		if r.GetFromTime().After(timestamp) {
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
