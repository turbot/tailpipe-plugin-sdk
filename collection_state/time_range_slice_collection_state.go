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
	objectRangeMap    map[string]*timeRangeCollectionState
	currentCollection *collectionMetadata
}

func (t *TimeRangeSliceCollectionState) IsEmpty() bool {
	for _, timeRange := range t.TimeRanges {
		if !timeRange.IsEmpty() {
			return false
		}
	}

	return true
}

func NewTimeRangeSliceCollectionState(collection *collectionMetadata, order CollectionOrder) *TimeRangeSliceCollectionState {
	res := &TimeRangeSliceCollectionState{
		Order:             order,
		currentCollection: collection,
		objectRangeMap:    make(map[string]*timeRangeCollectionState),
	}

	// initialise the active range
	res.updateActiveRange(collection.from)
	return res
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

func (t *TimeRangeSliceCollectionState) OnCollectionStarted(fromTime, toTime time.Time) error {
	// set the start time of the first range to the From time of the collection
	t.currentCollection = &collectionMetadata{
		from: fromTime,
		to:   toTime,
	}
	t.updateActiveRange(t.currentCollection.from)
	return nil

}

// OnCollectionComplete sets the end time of the collect - this is called from OnCollectionCOmplete after a successful collection
// we set the end time as we know that we have collected all data up to the collection 'To' time
// it sets the end time of the active range to the given end time
func (t *TimeRangeSliceCollectionState) OnCollectionComplete() error {
	if t.currentCollection == nil {
		return fmt.Errorf("cannot complete collection - no current collection set, OnCollectionStarted must be called first")
	}

	t.activeRange.setEndTime(t.currentCollection.to)

	// TODO this should merge all ranges which touch start and end time of current collection (how do we know the start time?)
	//  maybe the TimeRangeSliceCollectionState need to store metadata for the current collection? active range, start time, end time, etc.
	// compact
	// todo pass start and end time?
	t.compact()

	return nil
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
}

// compact merges adjacent time ranges that can be merged
func (t *TimeRangeSliceCollectionState) compact() {
	// TODO use current collection from and to

	slog.Info("Compacting time ranges")
	if len(t.TimeRanges) < 2 {
		// nothing to merge
		return
	}

	var compactedRanges []*timeRangeCollectionState
	for i := 0; i < len(t.TimeRanges)-2; i += 2 {
		r1 := t.TimeRanges[i]
		r2 := t.TimeRanges[i+1]
		// if there is no overlap, we cannot merge these ranges
		if r2.GetStartTime().After(r1.GetEndTime()) {
			slog.Info(fmt.Sprintf("Not merging time ranges %d and %d", i, i+2), "left range end time", r1.GetEndTime(), "right range start time", r2.GetStartTime())
			continue
		}

		// tell r1 to merge with r2 - r1 will now include r2
		r1.merge(r1)
		// add r1 to the compacted ranges slice
		compactedRanges = append(compactedRanges, r1)
		// if we get here, we can merge these two ranges
		slog.Info(fmt.Sprintf("Merging time ranges %d and %d", i, i+2), "left range end time", r1.GetEndTime(), "right range start time", r2.GetStartTime(), "right range end time", r2.GetEndTime(), "merged range end time", r1.GetEndTime())

	}
	// now update the TimeRanges slice with the compacted ranges
	if len(compactedRanges) == 0 {
		slog.Info("No time ranges could be merged")
		return
	}
	t.TimeRanges = compactedRanges
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
