package collection_state

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type TimeRangeCollectionState struct {
	TimeRanges  []*TimeRangeObjectState `json:"time_ranges"`
	Granularity time.Duration           `json:"granularity"`
	Order       CollectionOrder         `json:"order"`

	// activeRange represents the time range for the most recent object passed to ShouldCollect
	// we expect ShouldCollect to be called with objects in time order
	// the active range is extended to cover the timestamp of any object passed to ShouldCollect
	// if the timestamp for an object is contained within a different time range,
	// the active range is set to that time range.
	// NOTE: all ranges within the currentCollectionTimeRange will be merged by the final call to compact
	activeRange *TimeRangeObjectState

	// map of object identifier to collection state which contains the object
	// used to store the range associated with each object between the ShouldCollect call and the OnCollected call
	// (this is required because we do not want to have to recompute the range for each object on every OnCollected call)
	// NOTE: the map entry is cleared after OnCollected is called to minimise memory usage
	objectRangeMap             map[string]*TimeRangeObjectState
	currentCollectionTimeRange *CollectionTimeRange
}

func NewTimeRangeCollectionState() CollectionState {
	return &TimeRangeCollectionState{
		TimeRanges:     make([]*TimeRangeObjectState, 0),
		objectRangeMap: make(map[string]*TimeRangeObjectState),
	}
}

// NewReverseOrderTimeRangeSliceCollectionState creates a new TimeRangeCollectionState with reverse order
func NewReverseOrderTimeRangeSliceCollectionState() CollectionState {
	return &TimeRangeCollectionState{
		objectRangeMap: make(map[string]*TimeRangeObjectState),
		Order:          CollectionOrderReverse,
	}
}

// NewTimeRangeCollectionStateFromLegacy constructs a new TimeRangeCollectionState from a legacy state
func NewTimeRangeCollectionStateFromLegacy(legacy *TimeRangeCollectionStateLegacy) *TimeRangeCollectionState {
	state := &TimeRangeCollectionState{
		objectRangeMap: make(map[string]*TimeRangeObjectState),
	}
	// Convert the single legacy time range to the new format
	state.addRangeFromLegacy(legacy)
	return state
}

// Init is called after loading a collection state
// compact the time ranges in case the previous collection was not completed successfully
// (normally the state is compacted before the final save but if the process was killed before that,
// we may have a collection state with multiple ranges that can be merged)
func (t *TimeRangeCollectionState) Init(collectionTimeRange CollectionTimeRange) {
	// initialise the active range - this will create a new range for the collectionTimeRange from time if needed
	t.setCurrentCollectionTimeRange(collectionTimeRange)

	// perform initial compact - in case the saved data was uncompacted
	t.compact()

	// create map for object ranges if needed (i.e. if we were loaded from file)
	if t.objectRangeMap == nil {
		t.objectRangeMap = make(map[string]*TimeRangeObjectState)
	}
}

func (t *TimeRangeCollectionState) IsEmpty() bool {
	for _, timeRange := range t.TimeRanges {
		if !timeRange.IsEmpty() {
			return false
		}
	}

	return true
}

// OnCollectionComplete sets the end time of the collect - this is called  after a successful collection
// we the end time as we know that we have collected all data up to the collection 'to' time
// it sets the upper boundary (end time) of the active range to the upper boundary time of the collection time range
func (t *TimeRangeCollectionState) OnCollectionComplete() error {
	if t.currentCollectionTimeRange == nil {
		return fmt.Errorf("cannot complete collection - no current collection set, Init must be called first")
	}
	// set the upper boundary time of the active range to the upper boundary time of the collection time range
	t.activeRange.setUpperBoundaryTime(t.currentCollectionTimeRange.upperBoundaryTime())

	// perform a compact to merge any adjacent time ranges that can be merged
	t.compactForCollectionPeriod()

	return nil
}

func (t *TimeRangeCollectionState) SetGranularity(granularity time.Duration) {
	t.Granularity = granularity
	// set the granularity for all existing time ranges
	// TODO it would be nice to set the granularity when we create the state - see if we can do this
	for _, timeRange := range t.TimeRanges {
		timeRange.SetGranularity(granularity)
	}
}

func (t *TimeRangeCollectionState) GetGranularity() time.Duration {
	return t.Granularity
}

func (t *TimeRangeCollectionState) GetFromTime() time.Time {
	if len(t.TimeRanges) == 0 {
		return time.Time{}
	}
	return t.TimeRanges[0].GetFromTime()
}

func (t *TimeRangeCollectionState) GetToTime() time.Time {
	if len(t.TimeRanges) == 0 {
		return time.Time{}
	}
	return t.TimeRanges[len(t.TimeRanges)-1].GetToTime()
}

func (t *TimeRangeCollectionState) ShouldCollect(id string, timestamp time.Time) bool {
	// does the timestamp fall within the current collection time range?
	// NOTE the upper boundary is exclusive, so we check that the timestamp is on or inside the lower boundary but inside the upper boundary
	withinCollectionTimeRange := t.currentCollectionTimeRange.onOrInsideLowerBoundary(timestamp) && t.currentCollectionTimeRange.insideUpperBoundary(timestamp)
	// if the timestamp is outside the current collection time range, we should not collect
	if !withinCollectionTimeRange {
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

func (t *TimeRangeCollectionState) OnCollected(id string, timestamp time.Time) error {
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
func (t *TimeRangeCollectionState) MigrateFromLegacyState(bytes []byte) error {
	// the legacy state will be either	TimeRangeCollectionStateLegacy or ReverseOrderCollectionStateLegacy
	// try both in turn

	// First, try to unmarshal as TimeRangeCollectionStateLegacy
	var timeRangeLegacy TimeRangeCollectionStateLegacy
	if err := json.Unmarshal(bytes, &timeRangeLegacy); err == nil {
		// Validate that this is actually a TimeRangeCollectionStateLegacy by checking for FirstEntryTime
		if !timeRangeLegacy.FirstEntryTime.IsZero() {
			// Successfully unmarshaled as time range legacy state
			t.addRangeFromLegacy(&timeRangeLegacy)
			return nil
		}
	}

	// If that failed, try to unmarshal as ReverseOrderCollectionStateLegacy
	var reverseOrderLegacy ReverseOrderCollectionStateLegacy
	if err := json.Unmarshal(bytes, &reverseOrderLegacy); err == nil {
		// Successfully unmarshaled as reverse order legacy state
		t.populateFromReverseOrderLegacy(&reverseOrderLegacy)
		return nil
	}

	// If both failed, return an error
	return fmt.Errorf("failed to unmarshal legacy collection state - not a recognized legacy format")
}

func (t *TimeRangeCollectionState) Validate() error {
	var errorList []error
	for _, timeRange := range t.TimeRanges {
		if err := timeRange.Validate(); err != nil {
			errorList = append(errorList, err)
		}
	}
	if len(errorList) > 0 {
		return fmt.Errorf("validation failed for TimeRangeCollectionState: %w", errors.Join(errorList...))
	}
	return nil
}

// Compare compares the current state with another TimeRangeCollectionState and returns whether they are equal
// and a message describing any differences
func (t *TimeRangeCollectionState) Compare(want *TimeRangeCollectionState) (bool, string) {

	if len(t.TimeRanges) != len(want.TimeRanges) {
		return false, fmt.Sprintf("range count = %v, want %v", len(t.TimeRanges), len(want.TimeRanges))
	}
	for i, expected := range want.TimeRanges {
		if i >= len(t.TimeRanges) {
			return false, fmt.Sprintf("missing range at index %v", i)
		}
		if equal, msg := t.TimeRanges[i].Compare(expected); !equal {
			return false, msg
		}
	}

	if want.activeRange != nil {
		if ok, msg := t.activeRange.Compare(want.activeRange); !ok {
			return false, fmt.Sprintf("active range mismatch: %s", msg)
		}
	}
	return true, ""
}

// Clear updates the state clear any entries for the given time range.
func (t *TimeRangeCollectionState) Clear(clearRange CollectionTimeRange) {

	// create a new slice to hold the processed ranges
	var processedRanges []*TimeRangeObjectState

	// NOTE: truncate the clear time range 'To' to the granularity
	// this is to handle the case when we are recollecting for just today.
	// For example, when collecting with no from time at 2023-10-10T12:00:00
	// 		granularity: 1 day
	// 		collection state from: 2023-10-01T00:00:00, to: 2023-10-10T00:00:00
	// 		clear range from: 2023-10-10T00:00:00 to 2023-10-10T12:00:00
	//
	// we truncate the clear 'To' time by granularity of 1 day, so it becomes 2023-10-10T00:00:00
	// this then falls into the clearRange.IsRangeSubsumed case, which results in the end objects being cleared
	// but no other changes being made to the range
	clearRange.To = clearRange.To.Truncate(t.Granularity)

	slog.Info("Clear collection state for time range", "from", clearRange.From, "to", clearRange.To)
	defer slog.Info("Finished clearing collection state", "original_ranges", len(t.TimeRanges), "final_ranges", len(processedRanges))

	for _, timeRangeObjectState := range t.TimeRanges {
		// 4 cases:
		// 1. timeRangeObjectState is totally subsumed by the clearRange
		// 2. the clearRange is entirely contained within timeRangeObjectState
		// 3. the clearRange overlaps the start of timeRangeObjectState
		// 4. the clearRange overlaps the end of timeRangeObjectState

		switch {
		case timeRangeObjectState.TimeRange.IsRangeSubsumed(clearRange):
			slog.Debug("Time range object state is subsumed by clear range - deleting", "time range from", timeRangeObjectState.TimeRange.From, "to", timeRangeObjectState.TimeRange.To, "clear range from", clearRange.From, "to", clearRange.To)
			// this timeRangeObjectState is entirely within the clear range - delete (i.e. do not add to processedRanges)
			continue
		case clearRange.IsRangeSubsumed(timeRangeObjectState.TimeRange):
			slog.Debug("Clear range is subsumed by time range object state - splitting into two ranges", "time range from", timeRangeObjectState.TimeRange.From, "to", timeRangeObjectState.TimeRange.To, "clear range from", clearRange.From, "to", clearRange.To)

			// if the clear range is totally contained within the timeRangeObjectState, we ned toi create 2 new ranges:
			// 1. a new range from the start of timeRangeObjectState to the start of clearRange
			// 2. a new range from the end of clearRange to the end of timeRangeObjectState (including the end objects)
			// create the first range from the start of timeRangeObjectState to the start of clearRange

			// clone the timeRangeObjectState to create a new range
			startRange := timeRangeObjectState.Clone()
			// set the to time to the start of the clear range
			startRange.TimeRange.To = clearRange.From
			// clear the end objects as they are not relevant for this range
			startRange.EndObjects = make(map[string]struct{})

			// and the end range
			endRange := timeRangeObjectState.Clone()
			// set the from time to the end of the clear range
			endRange.TimeRange.From = clearRange.To

			// add the  ranges to the processed ranges
			// NOTE: if the clear range starts or ends on our start or end time,
			// then either startRange or endRange will be empty - do not add the empty range
			if !startRange.IsEmpty() {
				processedRanges = append(processedRanges, startRange)
			}
			if !endRange.IsEmpty() {
				processedRanges = append(processedRanges, endRange)
			}

		case clearRange.OverlapsEnd(timeRangeObjectState.TimeRange):
			// if the clear range START overlaps the END of the timeRangeObjectState, we need to update the end time
			// update our end time to the start of the clear range
			// and clear our end objects
			slog.Debug("Time range object state overlaps start of clear range - updating end time and clearing end objects", "time range from", timeRangeObjectState.TimeRange.From, "to", timeRangeObjectState.TimeRange.To, "clear range from", clearRange.From, "to", clearRange.To)
			timeRangeObjectState.TimeRange.To = clearRange.From
			timeRangeObjectState.EndObjects = make(map[string]struct{})
			processedRanges = append(processedRanges, timeRangeObjectState)

		case clearRange.OverlapsStart(timeRangeObjectState.TimeRange):
			// if the clear range END overlaps the START of the timeRangeObjectState, we need to update the start time
			slog.Debug("Time range object state overlaps end of clear range - updating start time and keeping end objects", "time range from", timeRangeObjectState.TimeRange.From, "to", timeRangeObjectState.TimeRange.To, "clear range from", clearRange.From, "to", clearRange.To)
			timeRangeObjectState.TimeRange.From = clearRange.To
			// we do not clear the end objects as they are still relevant for this range
			processedRanges = append(processedRanges, timeRangeObjectState)

		default:
			slog.Debug("Time range object state does not overlap with clear range - keeping as is", "time range from", timeRangeObjectState.TimeRange.From, "to", timeRangeObjectState.TimeRange.To, "clear range from", clearRange.From, "to", clearRange.To)
			// If we get here, the timeRangeObjectState does not overlap with the clearRange
			// add the timeRangeObjectState to the processed ranges as-is and continue
			processedRanges = append(processedRanges, timeRangeObjectState)
		}
	}

	// Now we have processed all ranges, we can set the TimeRanges to the processed ranges
	t.TimeRanges = processedRanges
}

// addRangeFromLegacy populates the state from a legacy TimeRangeCollectionStateLegacy
func (t *TimeRangeCollectionState) addRangeFromLegacy(legacy *TimeRangeCollectionStateLegacy) {
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
	newRange := &TimeRangeObjectState{
		TimeRange: CollectionTimeRange{
			From:            fromTime,
			To:              toTime,
			CollectionOrder: legacy.CollectionOrder,
		},
		EndObjects:  legacy.EndObjects,
		Granularity: legacy.Granularity,
	}

	// Add the new range to the state
	t.TimeRanges = append(t.TimeRanges, newRange)
}

// populateFromReverseOrderLegacy populates the state from ReverseOrderCollectionStateLegacy
func (t *TimeRangeCollectionState) populateFromReverseOrderLegacy(legacy *ReverseOrderCollectionStateLegacy) {
	// Convert each legacy time range to the new format
	for _, legacyRange := range legacy.TimeRanges {
		t.addRangeFromLegacy(legacyRange)
	}
}

// setCurrentCollectionTimeRange sets the current collection time range and updates the active range to the start of the collection time range
func (t *TimeRangeCollectionState) setCurrentCollectionTimeRange(tr CollectionTimeRange) {
	t.currentCollectionTimeRange = &tr
	// rather than pass 'from'  time (which we use for forward collection), pass the 'lower boundary time of the range
	// this resolves to the 'from' time for forward collection and the 'to' time for reverse collection
	t.updateActiveRange(t.currentCollectionTimeRange.lowerBoundaryTime())
}

// upperBoundaryTime returns the the furthest time in the direction of collection
// i.e. if we are collecting forwards, the upperBoundaryTime is the end time of the range,
// if we are collecting backwards, the upperBoundaryTime is the start time of the range
func (t *TimeRangeCollectionState) upperBoundaryTime() time.Time {
	if t.Order == CollectionOrderChronological {
		return t.GetToTime()
	}
	return t.GetFromTime()
}

// lowerBoundaryTime returns the the furthest time in the opposite direction of collection
func (t *TimeRangeCollectionState) lowerBoundaryTime() time.Time {
	if t.Order == CollectionOrderChronological {
		return t.GetFromTime()
	}
	return t.GetToTime()
}

// updateActiveRange determines the time range 'associated' with the given timestamp.
// If there is no active range:
//   - creates a new one for the timestamp.
//
// If there is an active range:
//   - if timestamp falls within a subsequent range, the current active range is extended to the start of that range
//     and the active range is updated to that range.
//   - if the timestamp does not fall within a different range, the active range remains unchangeed.
//     In OnCollected, it will be extended to the timestamp
func (t *TimeRangeCollectionState) updateActiveRange(timestamp time.Time) {
	// get the range for the timestamp
	rangeForTimestamp := t.rangeForTime(timestamp)

	// if we have an active range, check whether a different range contains the timestamp - if so update it
	if t.activeRange != nil {
		// if the timestamp is within the a different range, we need to update the active range
		if rangeForTimestamp != nil && rangeForTimestamp != t.activeRange {
			slog.Info("Updating active range for time",
				"timestamp", timestamp,
				"active range upper boundary time", t.activeRange.TimeRange.upperBoundaryTime(),
				"range for timestamp lower boundary time", rangeForTimestamp.TimeRange.lowerBoundaryTime())
			// if the range for the timestamp is different from the active range, we need to update the active range
			// set the end time of the active range to the start time of the next range so we merge them next time we
			// compact the state
			t.activeRange.setUpperBoundaryTime(rangeForTimestamp.TimeRange.lowerBoundaryTime())
			// use the next range as the active
			t.activeRange = rangeForTimestamp
		}

		// to get here, either:
		// - the timestamp is outside all current ranges (the most common case as we collect)
		// - the timestamp is within the current active range
		// In either case, leave the active range as is - it will be extended to the timestamp in OnCollected
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
func (t *TimeRangeCollectionState) compact() {
	// if there are less than 2 time ranges, we cannot compact
	if len(t.TimeRanges) < 2 {
		return
	}

	slog.Info("Compacting adjacent time ranges")
	var compactedRanges []*TimeRangeObjectState
	currentRange := t.TimeRanges[0]

	for i := 1; i < len(t.TimeRanges); i++ {
		nextRange := t.TimeRanges[i]

		// we will merge the ranges if they overlap
		rangesOverlap := currentRange.GetToTime().Compare(nextRange.GetFromTime()) >= 0

		// if the ranges do not overlap, we can add the current range to compacted ranges
		if !rangesOverlap {
			// add the current range to compacted ranges and move to next
			compactedRanges = append(compactedRanges, currentRange)
			currentRange = nextRange
			continue
		}

		// tell currentRange to merge with nextRange
		currentRange.merge(nextRange)
		slog.Info("Merging adjacent time ranges",
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

// compactForCollectionPeriod merges all time ranges which fall within the current collection period
func (t *TimeRangeCollectionState) compactForCollectionPeriod() {
	// currentCollectionTimeRange may not be set yet if this is being called from Init
	if t.currentCollectionTimeRange == nil {
		return
	}

	// if there are less than 2 time ranges, we cannot compact
	if len(t.TimeRanges) < 2 {
		return
	}

	slog.Info("Compacting time ranges within collection period")
	var compactedRanges []*TimeRangeObjectState
	currentRange := t.TimeRanges[0]

	for i := 1; i < len(t.TimeRanges); i++ {
		nextRange := t.TimeRanges[i]

		// we will merge the ranges if EITHER:
		//  - if the ranges overlap, OR
		//  - if the ranges fall within the collection period (i.e. the collection from time is before the end time of currentRange

		rangesOverlap := currentRange.GetToTime().Compare(nextRange.GetFromTime()) >= 0

		rangesFallWithinCollectionPeriod := currentRange.GetToTime().After(t.currentCollectionTimeRange.From) &&
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
		slog.Info("Merging time ranges within collection period",
			"left range end time", currentRange.GetToTime(),
			"right range start time", nextRange.GetFromTime(),
			"right range end time", nextRange.GetToTime(),
			"merged range end time", currentRange.GetToTime())
	}

	// add the last range
	compactedRanges = append(compactedRanges, currentRange)

	// now update the TimeRanges slice with the compacted ranges
	if len(compactedRanges) == 0 {
		slog.Info("No time ranges could be merged within collection period")
		return
	}
	t.TimeRanges = compactedRanges
}

// rangeForTime returns the index of the time range that contains the given timestamp or nil if no such range exists.
// we expect ranges will not overlap, so we can return the first range that contains the timestamp
// NOTE: we DO NOT need to take collection order into account
func (t *TimeRangeCollectionState) rangeForTime(timestamp time.Time) *TimeRangeObjectState {
	for _, r := range t.TimeRanges {
		// if the timestamp is within the range, return the range
		// NOTE: in this case the upper boundary IS included - as we will extend the range to include the timestamp
		if r.TimeRange.onOrInsideLowerBoundary(timestamp) && r.TimeRange.onOrInsideUpperBoundary(timestamp) {
			return r
		}
	}

	return nil
}

// addRange creates a new time range for the given timestamp and adds it to the collection in the correct position
func (t *TimeRangeCollectionState) addRange(timestamp time.Time) *TimeRangeObjectState {
	// create a new time range
	newRange := newTimeRangeCollectionState(timestamp, t.Order)
	newRange.SetGranularity(t.Granularity)
	// find the appropriate location to insert the new range into our list
	for i, r := range t.TimeRanges {
		if r.GetFromTime().After(timestamp) {
			// insert the new range before this one
			t.TimeRanges = append(t.TimeRanges[:i], append([]*TimeRangeObjectState{newRange}, t.TimeRanges[i:]...)...)
			return newRange
		}
	}
	// if we get here, the new range should be added to the end of the list
	t.TimeRanges = append(t.TimeRanges, newRange)

	return newRange
}

func (t *TimeRangeCollectionState) String() string {
	// return a string representation of the collection state
	// this is used for debugging and logging
	var ranges []string
	for _, r := range t.TimeRanges {
		ranges = append(ranges, r.String())
	}
	return fmt.Sprintf("TimeRangeCollectionState{TimeRanges: [%s], Granularity: %s, Order: %d, ActiveRange: %s}",
		ranges, t.Granularity, t.Order, t.activeRange)

}
