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
	// if the timestamp is outside the current collection time range, we should not collect
	if t.currentCollectionTimeRange != nil && !t.currentCollectionTimeRange.Contains(timestamp) {
		return false
	}
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
	// get the range for the timestamp
	rangeForTimestamp := t.rangeForTime(timestamp)

	// if we have an active range, check whether a different range contains the timestamp - if so update it
	if t.activeRange != nil {
		if rangeForTimestamp != nil && rangeForTimestamp != t.activeRange {
			slog.Info("Updating active range for time", "timestamp", timestamp, "active range end time", t.activeRange.GetToTime(), "range for timestamp start time", rangeForTimestamp.GetFromTime())
			// if the range for the timestamp is different from the active range, we need to update the active range
			// set the end time of the active range to the start time of the next range so we merge them next time we
			// compact the state
			t.activeRange.setEndTime(rangeForTimestamp.GetFromTime())
			// use the next range as the active
			t.activeRange = rangeForTimestamp
		}
		return
	}

	// so we have no active range - did we find a range for the timestamp? If not, create a new one
	if rangeForTimestamp == nil {
		// no range for the timestamp - create a new one
		rangeForTimestamp = t.addRange(timestamp)
		slog.Info("Created new active range for time", "timestamp", timestamp, "active range start time", rangeForTimestamp.GetFromTime(), "active range end time", rangeForTimestamp.GetToTime())
	} else {
		slog.Info("Found existing active range for time", "timestamp", timestamp, "active range start time", rangeForTimestamp.GetFromTime(), "active range end time", rangeForTimestamp.GetToTime())
	}

	// now update the active range to the range for the timestamp
	t.activeRange = rangeForTimestamp
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

// rangeForTime returns the index of the time range that contains the given timestamp or nil if no such range exists.
func (t *TimeRangeSliceCollectionState) rangeForTime(timestamp time.Time) *timeRangeCollectionState {
	for _, r := range t.TimeRanges {
		if r.Contains(timestamp) {
			slog.Info("Found existing range for time", "timestamp", timestamp, "range From", r.From, "range TO", r.To)
			return r
		}
		slog.Info("Range does not contain time", "timestamp", timestamp, "range From", r.From, "range TO", r.To)
	}
	return nil
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
