package collection_state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

type TimeRangeSliceCollectionState struct {
	TimeRanges  []*timeRangeObjectState `json:"TimeRanges"`
	Granularity time.Duration           `json:"granularity"`
	Order       CollectionOrder         `json:"order"`

	// activeRange represents the time range for the most recent object passed to ShouldCollect
	// we expect ShouldCollect to be called with objects in time order
	// the active range is extended to cover the timestamp of any object passed to ShouldCollect
	// if the timestamp for an object is contained within a different time range,
	// the active range is set to that time range.
	// NOTE: all ranges within the currentCollectionTimeRange will be merged by the final call to compact
	activeRange *timeRangeObjectState

	// map of object identifier to collection state which contains the object
	// used to store the range associated with each object between the ShouldCollect call and the OnCollected call
	// (this is required because we do not want to have to recompute the range for each object on every OnCollected call)
	// NOTE: the map entry is cleared after OnCollected is called to minimise memory usage
	objectRangeMap             map[string]*timeRangeObjectState
	currentCollectionTimeRange *TimeRange
}

func NewTimeRangeSliceCollectionState() CollectionState {
	return &TimeRangeSliceCollectionState{
		objectRangeMap: make(map[string]*timeRangeObjectState),
	}
}

// NewReverseOrderTimeRangeSliceCollectionState creates a new TimeRangeSliceCollectionState with reverse order
func NewReverseOrderTimeRangeSliceCollectionState() CollectionState {
	return &TimeRangeSliceCollectionState{
		objectRangeMap: make(map[string]*timeRangeObjectState),
		Order:          CollectionOrderReverse,
	}
}

// NewTimeRangeSliceCollectionStateFromLegacy constructs a new TimeRangeSliceCollectionState from a legacy state
func NewTimeRangeSliceCollectionStateFromLegacy(legacy *TimeRangeCollectionStateLegacy) *TimeRangeSliceCollectionState {
	state := &TimeRangeSliceCollectionState{
		objectRangeMap: make(map[string]*timeRangeObjectState),
	}
	// Convert the single legacy time range to the new format
	state.populateFromLegacy(legacy)
	return state
}

// Init is called after loading a collection state
// compact the time ranges in case the previous collection was not completed successfully
// (normally the state is compacted before the final save but if the process was killed before that,
// we may have a collection state with multiple ranges that can be merged)
func (t *TimeRangeSliceCollectionState) Init(collectionTimeRange *TimeRange) error {
	// initialise the active range - this will create a new range for the collectionTimeRange from time if needed
	t.setTimeRange(collectionTimeRange)

	// perform initial compact
	t.compact()

	// create map for object ranges if needed (i.e. if we were loaded from file)
	if t.objectRangeMap == nil {
		t.objectRangeMap = make(map[string]*timeRangeObjectState)
	}

	return nil
}

func (t *TimeRangeSliceCollectionState) IsEmpty() bool {
	for _, timeRange := range t.TimeRanges {
		if !timeRange.IsEmpty() {
			return false
		}
	}

	return true
}

// OnCollectionStarted is called when a new collection is started - set the currentCollectionTimeRange
// and initialise the active range
func (t *TimeRangeSliceCollectionState) OnCollectionStarted(fromTime, toTime time.Time) {
	t.setTimeRange(&TimeRange{
		From: fromTime,
		To:   toTime,
	})
}

// OnCollectionComplete sets the end time of the collect - this is called  after a successful collection
// we the end time as we know that we have collected all data up to the collection 'to' time
// it sets the upper boundary (end time) of the active range to the upper boundary time of the collection time range
func (t *TimeRangeSliceCollectionState) OnCollectionComplete() error {
	if t.currentCollectionTimeRange == nil {
		return fmt.Errorf("cannot complete collection - no current collection set, OnCollectionStarted must be called first")
	}

	t.activeRange.setUpperBoundaryTime(t.currentCollectionTimeRange.upperBoundaryTime(t.Order))

	// perform a compact to merge any adjacent time ranges that can be merged
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
	if t.currentCollectionTimeRange != nil && !t.currentCollectionTimeRange.Contains(timestamp, t.Order) {
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

// MigrateFromLegacyState attempts to migrate from a legacy collection state
func (t *TimeRangeSliceCollectionState) MigrateFromLegacyState(bytes []byte) error {
	// the legacy state will be either 	TimeRangeCollectionStateLegacy or ReverseOrderCollectionStateLegacy
	// try both in turn

	// First, try to unmarshal as TimeRangeCollectionStateLegacy
	var timeRangeLegacy TimeRangeCollectionStateLegacy
	if err := json.Unmarshal(bytes, &timeRangeLegacy); err == nil {
		// Validate that this is actually a TimeRangeCollectionStateLegacy by checking for FirstEntryTime
		if !timeRangeLegacy.FirstEntryTime.IsZero() {
			// Successfully unmarshaled as time range legacy state
			t.populateFromLegacy(&timeRangeLegacy)
			return nil
		}
	}

	// If that failed, try to unmarshal as ReverseOrderCollectionStateLegacy
	var reverseOrderLegacy ReverseOrderCollectionStateLegacy
	if err := json.Unmarshal(bytes, &reverseOrderLegacy); err == nil {
		// Validate that this is actually a ReverseOrderCollectionStateLegacy by checking for actual time range data
		if len(reverseOrderLegacy.TimeRanges) > 0 && !reverseOrderLegacy.TimeRanges[0].FirstEntryTime.IsZero() {
			// Successfully unmarshaled as reverse order legacy state
			t.populateFromReverseOrderLegacy(&reverseOrderLegacy)
			return nil
		}
	}

	// If both failed, return an error
	return fmt.Errorf("failed to unmarshal legacy collection state - not a recognized legacy format")
}

// populateFromLegacy populates the state from a legacy TimeRangeCollectionStateLegacy
func (t *TimeRangeSliceCollectionState) populateFromLegacy(legacy *TimeRangeCollectionStateLegacy) {
	// Set the order and granularity on the main state
	t.Order = legacy.CollectionOrder
	t.Granularity = legacy.Granularity

	// Determine the From and to times based on the legacy structure
	var fromTime, toTime time.Time

	if legacy.CollectionOrder == CollectionOrderChronological {
		// For chronological order, use FirstEntryTime as From and EndTime as to
		fromTime = legacy.FirstEntryTime
		toTime = legacy.EndTime
	} else {
		// For reverse order, use LastEntryTime as From and FirstEntryTime as to
		fromTime = legacy.LastEntryTime
		toTime = legacy.FirstEntryTime
	}

	// Create the new time range object state
	newRange := &timeRangeObjectState{
		From:            fromTime,
		To:              toTime,
		EndObjects:      legacy.EndObjects,
		Granularity:     legacy.Granularity,
		CollectionOrder: legacy.CollectionOrder,
	}

	// Add the new range to the state
	t.TimeRanges = append(t.TimeRanges, newRange)
}

// populateFromReverseOrderLegacy populates the state from ReverseOrderCollectionStateLegacy
func (t *TimeRangeSliceCollectionState) populateFromReverseOrderLegacy(legacy *ReverseOrderCollectionStateLegacy) {
	// Convert each legacy time range to the new format
	for _, legacyRange := range legacy.TimeRanges {
		t.populateFromLegacy(legacyRange)
	}

	// Ensure all time ranges have the same granularity
	if len(t.TimeRanges) > 0 {
		t.SetGranularity(t.Granularity)
	}
}

// setTimeRange sets the current collection time range and updates the active range to the start of the collection time range
func (t *TimeRangeSliceCollectionState) setTimeRange(tr *TimeRange) {
	t.currentCollectionTimeRange = tr
	// rather than pass 'from'  time (which we use for forward collection), pass the 'lower boundary time of the range
	// this resolves to the 'from' time for forward collection and the 'to' time for reverse collection
	t.updateActiveRange(t.currentCollectionTimeRange.lowerBoundaryTime(t.Order))
}

// upperBoundaryTime returns the the furthest time in the direction of collection
// i.e. if we are collecting forwards, the upperBoundaryTime is the end time of the range,
// if we are collecting backwards, the upperBoundaryTime is the start time of the range
func (t *TimeRangeSliceCollectionState) upperBoundaryTime() time.Time {
	if t.Order == CollectionOrderChronological {
		return t.GetToTime()
	}
	return t.GetFromTime()
}

// lowerBoundaryTime returns the the furthest time in the opposite direction of collection
func (t *TimeRangeSliceCollectionState) lowerBoundaryTime() time.Time {
	if t.Order == CollectionOrderChronological {
		return t.GetFromTime()
	}
	return t.GetToTime()
}

// updateActiveRange determines the time range containing the given timestamp
// If there is no active range, it creates a new one for the timestamp.
// If the range following active range (in the direction of collection) contains the timestamp, the active range is updated
// to this range
func (t *TimeRangeSliceCollectionState) updateActiveRange(timestamp time.Time) {
	// get the range for the timestamp
	rangeForTimestamp := t.rangeForTime(timestamp)

	// if we have an active range, check whether a different range contains the timestamp - if so update it
	if t.activeRange != nil {
		if rangeForTimestamp != nil && rangeForTimestamp != t.activeRange {
			slog.Info("Updating active range for time",
				"timestamp", timestamp,
				"active range upper boundary time", t.activeRange.upperBoundaryTime(),
				"range for timestamp lower boundary time", rangeForTimestamp.lowerBoundaryTime())
			// if the range for the timestamp is different from the active range, we need to update the active range
			// set the end time of the active range to the start time of the next range so we merge them next time we
			// compact the state
			t.activeRange.setUpperBoundaryTime(rangeForTimestamp.lowerBoundaryTime())
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
	var compactedRanges []*timeRangeObjectState
	currentRange := t.TimeRanges[0]

	for i := 1; i < len(t.TimeRanges); i++ {
		nextRange := t.TimeRanges[i]

		// we will merge the ranges if EITHER:
		//  - if the ranges overlap, OR
		//  - if the ranges fall within the collection period (i.e. the collection from time is before the end time of currentRange

		rangesOverlap := currentRange.GetToTime().Compare(nextRange.GetFromTime()) >= 0

		rangesFallWithinCollectionPeriod := t.currentCollectionTimeRange != nil &&
			currentRange.GetToTime().After(t.currentCollectionTimeRange.From) &&
			nextRange.GetFromTime().Before(t.currentCollectionTimeRange.To)

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
}

// rangeForTime returns the index of the time range that contains the given timestamp or nil if no such range exists.
// we expect ranges will not overlap, so we can return the first range that contains the timestamp
// NOTE: we DO NOT need to take collection order into account
func (t *TimeRangeSliceCollectionState) rangeForTime(timestamp time.Time) *timeRangeObjectState {
	for _, r := range t.TimeRanges {
		if r.Contains(timestamp) {
			slog.Debug("Found existing range for time", "timestamp", timestamp, "range from", r.From, "range TO", r.To)
			return r
		}
	}
	slog.Debug("No existing range found for time", "timestamp", timestamp)
	return nil
}

// addRange creates a new time range for the given timestamp and adds it to the collection in the correct position
func (t *TimeRangeSliceCollectionState) addRange(timestamp time.Time) *timeRangeObjectState {
	// create a new time range
	newRange := newTimeRangeCollectionState(timestamp, t.Order)
	newRange.SetGranularity(t.Granularity)
	// find the appropriate location to insert the new range into our list
	for i, r := range t.TimeRanges {
		if r.GetFromTime().After(timestamp) {
			// insert the new range before this one
			t.TimeRanges = append(t.TimeRanges[:i], append([]*timeRangeObjectState{newRange}, t.TimeRanges[i:]...)...)
			return newRange
		}
	}
	// if we get here, the new range should be added to the end of the list
	t.TimeRanges = append(t.TimeRanges, newRange)

	return newRange
}
