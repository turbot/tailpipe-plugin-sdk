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

	// initialise the active range - this will create a new range for the collection From time if needed
	res.updateActiveRange(collection.from)
	return res
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
				// set the end time of the active range to the start time of the next range so we merge them next time we
				// compact the state
				t.activeRange.setEndTime(nextRange.GetStartTime())
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
func (t *TimeRangeSliceCollectionState) compact() error {
	if t.currentCollection == nil {
		return fmt.Errorf("cannot compact time ranges - no current collection set, OnCollectionStarted must be called first")
	}

	// if there are less than 2 time ranges, we cannot compact
	if len(t.TimeRanges) < 2 {
		return nil
	}

	slog.Info("Compacting time ranges")
	var compactedRanges []*timeRangeCollectionState
	currentRange := t.TimeRanges[0]

	for i := 1; i < len(t.TimeRanges); i++ {
		nextRange := t.TimeRanges[i]

		// if there is no overlap, we cannot merge these ranges
		// UNLESS the collection From time is before the end time of currentRange and c	ollection To time is after the start time of nextRange
		rangesFallWithinCollectionPeriod := currentRange.GetEndTime().After(t.currentCollection.from) &&
			nextRange.GetStartTime().Before(t.currentCollection.to)

		rangesOverlap := currentRange.GetEndTime().Compare(nextRange.GetStartTime()) >= 0

		slog.Debug("Checking time ranges for merging",
			"collection from", t.currentCollection.from,
			"collection to", t.currentCollection.to,
			"current range start time", currentRange.GetStartTime(),
			"current range end time", currentRange.GetEndTime(),
			"next range start time", nextRange.GetStartTime(),
			"next range end time", nextRange.GetEndTime(),
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
			"left range end time", currentRange.GetEndTime(),
			"right range start time", nextRange.GetStartTime(),
			"right range end time", nextRange.GetEndTime(),
			"merged range end time", currentRange.GetEndTime())
	}

	// add the last range
	compactedRanges = append(compactedRanges, currentRange)

	// now update the TimeRanges slice with the compacted ranges
	if len(compactedRanges) == 0 {
		slog.Info("No time ranges could be merged")
		return nil
	}
	t.TimeRanges = compactedRanges
	return nil
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
