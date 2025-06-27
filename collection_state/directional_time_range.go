package collection_state

import (
	"errors"
	"fmt"
	"time"
)

type DirectionalTimeRange struct {
	// LowerBoundary is the chronological start time of the range
	// For forward collection, this is the start time (inclusive)
	// For reverse collection, this is the end time (exclusive)
	LowerBoundary time.Time `json:"lower_boundary"`
	// UpperBoundary is the chronological end time of the range
	// For forward collection, this is the end time (exclusive)
	// For reverse collection, this is the start time (inclusive)
	UpperBoundary   time.Time       `json:"upperboundary"`
	CollectionOrder CollectionOrder `json:"collection_order"`
}

func (t *DirectionalTimeRange) Validate() error {
	if t.LowerBoundary.IsZero() {
		return errors.New("from time is zero")
	}
	if t.UpperBoundary.IsZero() {
		return errors.New("to time is zero")
	}
	if t.LowerBoundary.After(t.UpperBoundary) {
		return errors.New("from time is after to time")
	}
	if t.CollectionOrder != CollectionOrderChronological && t.CollectionOrder != CollectionOrderReverse {
		return fmt.Errorf("invalid collection order: %v", t.CollectionOrder)
	}
	return nil
}

// OverlapsEnd returns whether our START overlaps the END of the other time range
// NOTE:
// - returns true if our START is within the other range (but not if either range subsumes the other)
// - returns false if either range completely contains the other
func (t *DirectionalTimeRange) OverlapsEnd(other DirectionalTimeRange) bool {
	// Check if either range subsumes the other - if so, return false
	if t.IsRangeSubsumed(other) || other.IsRangeSubsumed(*t) {
		return false
	}

	// Check if our start time is within the other range
	// Our start should be after other's start but before other's end
	return t.LowerBoundary.After(other.LowerBoundary) && t.LowerBoundary.Before(other.UpperBoundary)
}

// OverlapsStart returns whether our END overlaps the START of the other time range
// NOTE:
// - returns true if our END is after the other's start and before the other's end, but not if either range subsumes the other
func (t *DirectionalTimeRange) OverlapsStart(other DirectionalTimeRange) bool {
	// Return false if either range subsumes the other
	if t.IsRangeSubsumed(other) || other.IsRangeSubsumed(*t) {
		return false
	}
	// Check if our end time overlaps with the other range
	return t.UpperBoundary.After(other.LowerBoundary) && t.LowerBoundary.Before(other.LowerBoundary)
}

// IsRangeSubsumed checks if this time range is completely contained within another time range
func (t *DirectionalTimeRange) IsRangeSubsumed(other DirectionalTimeRange) bool {
	return t.LowerBoundary.Compare(other.LowerBoundary) >= 0 && t.UpperBoundary.Compare(other.UpperBoundary) <= 0
}

// endTime returns the the furthest time in the direction of collection
// i.e. if we are collecting forwards, the endTime is the upperBoundary time
// if we are collecting backwards, the endTime is the lowerBoundary time
func (t *DirectionalTimeRange) endTime() time.Time {
	if t.CollectionOrder == CollectionOrderChronological {
		return t.UpperBoundary
	}
	return t.LowerBoundary
}

// startTime returns the the furthest time in the opposite direction of collection
// i.e. if we are collecting forwards, the startTime is the lowerBoundary time
// if we are collecting backwards, the startTime is the upperBoundary time
func (t *DirectionalTimeRange) startTime() time.Time {
	if t.CollectionOrder == CollectionOrderChronological {
		return t.LowerBoundary
	}
	return t.UpperBoundary
}

// afterStart returns whether the timestamp is after the start _in the direction of collection_
// for chronological collection, this returns whether the time is AFTER the lowerBoundary time
// for reverse collection, this returns whether the time is BEFORE the upperBoundary time
func (t *DirectionalTimeRange) afterStart(timestamp time.Time) bool {
	if t.CollectionOrder == CollectionOrderChronological {
		return timestamp.After(t.LowerBoundary)
	}
	return timestamp.Before(t.UpperBoundary)
}

// beforeEnd returns whether the timestamp is before the upper boundary time (i.e. in the opposite to collection direction_)
// for chronological collection, this returns whether the time is BEFORE the upperBoundary time
// for reverse collection, this returns whether the time is AFTER the lowerBoundary time
func (t *DirectionalTimeRange) beforeEnd(timestamp time.Time) bool {

	if t.CollectionOrder == CollectionOrderChronological {
		return timestamp.Before(t.UpperBoundary)
	}
	return timestamp.After(t.LowerBoundary)
}

// beforeStart returns whether the timestamp is before the lower boundary time (i.e. in the opposite to collection direction_)
// for chronological collection, this returns whether the time is BEFORE the lowerBoundary time
// for reverse collection, this returns whether the time is AFTER the upperBoundary time
func (t *DirectionalTimeRange) beforeStart(timestamp time.Time) bool {
	if t.CollectionOrder == CollectionOrderChronological {
		return timestamp.Before(t.LowerBoundary)
	}
	return timestamp.After(t.UpperBoundary)
}

// afterEnd returns whether the timestamp is outside the upper boundary time
// for chronological collection, this returns whether the time is AFTER the upperBoundary time
// for reverse collection, this returns whether the time is BEFORE the lowerBoundary time
func (t *DirectionalTimeRange) afterEnd(timestamp time.Time) bool {
	if t.CollectionOrder == CollectionOrderChronological {
		return timestamp.After(t.UpperBoundary)
	}
	return timestamp.Before(t.LowerBoundary)
}

// onOrAfterStart returns whether the timestamp is on or after the lower boundary time (in the direction of collection)
// for chronological collection, this returns whether the time is ON or AFTER the lowerBoundary time
// for reverse collection, this returns whether the time is ON or BEFORE the upperBoundary time
func (t *DirectionalTimeRange) onOrAfterStart(timestamp time.Time) bool {
	return t.startTime().Equal(timestamp) || t.afterStart(timestamp)
}

//	onOrBeforeEnd returns whether the timestamp is on or before the end time (in the opposite direction of collection)
//
// for chronological collection, this returns whether the time is ON or BEFORE the upperBoundary time
// for reverse collection, this returns whether the time is ON or AFTER the lowerBoundary time
func (t *DirectionalTimeRange) onOrBeforeEnd(timestamp time.Time) bool {
	return t.endTime().Equal(timestamp) || t.beforeEnd(timestamp)
}

// extendEndTime extends the end time of the collection range, if the new teem is beyond the current time
// (in the direction of collection).
func (t *DirectionalTimeRange) extendEndTime(newTime time.Time) {

	if t.CollectionOrder == CollectionOrderChronological {
		// only set if the new time extends the range
		if newTime.After(t.UpperBoundary) {
			t.UpperBoundary = newTime
		}
	} else {
		// for reverse collection, only set if the new time extends the range (backwards in time)
		// the upper boundary for reverse collection is the LowerBoundary time
		// to extend the range, we need to set LowerBoundary to an earlier time
		if newTime.Before(t.LowerBoundary) {
			t.LowerBoundary = newTime
		}
	}
}
