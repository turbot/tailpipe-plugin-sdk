package collection_state

import (
	"errors"
	"fmt"
	"time"
)

type CollectionTimeRange struct {
	From            time.Time
	To              time.Time
	CollectionOrder CollectionOrder
}

// upperBoundaryTime returns the the furthest time in the direction of collection
// i.e. if we are collecting forwards, the upperBoundaryTime is the end time of the range,
// if we are collecting backwards, the upperBoundaryTime is the start time of the range
func (t *CollectionTimeRange) upperBoundaryTime() time.Time {

	if t.CollectionOrder == CollectionOrderChronological {
		return t.To
	}
	return t.From
}

// lowerBoundaryTime returns the the furthest time in the opposite direction of collection
// i.e. if we are collecting forwards, the lowerBoundaryTime is the start time of the range,
// if we are collecting backwards, the lowerBoundaryTime is the end time of the range
func (t *CollectionTimeRange) lowerBoundaryTime() time.Time {

	if t.CollectionOrder == CollectionOrderChronological {
		return t.From
	}
	return t.To
}

// insideLowerBoundary returns whether the timestamp is inside the lower boundary time (exclusive, i.e. NOT including the boundary time itself)
// for chronological collection, this returns whether the time is AFTER the start time
// for reverse collection, this returns whether the time is BEFORE the end time
func (t *CollectionTimeRange) insideLowerBoundary(timestamp time.Time) bool {

	if t.CollectionOrder == CollectionOrderChronological {
		return timestamp.After(t.From)
	}
	return timestamp.Before(t.To)
}

// insideUpperBoundary returns whether the timestamp is inside the upper boundary time (exclusive, i.e. NOT including the boundary time itself)
// for chronological collection, this returns whether the time is BEFORE the end time
// for reverse collection, this returns whether the time is AFTER the start time
func (t *CollectionTimeRange) insideUpperBoundary(timestamp time.Time) bool {

	if t.CollectionOrder == CollectionOrderChronological {
		return timestamp.Before(t.To)
	}
	return timestamp.After(t.From)
}

// outsideLowerBoundary returns whether the timestamp is outside the lower boundary time
// for chronological collection, this returns whether the time is BEFORE the start time
// for reverse collection, this returns whether the time is AFTER the end time
func (t *CollectionTimeRange) outsideLowerBoundary(timestamp time.Time) bool {

	if t.CollectionOrder == CollectionOrderChronological {
		return timestamp.Before(t.From)
	}
	return timestamp.After(t.To)
}

// outsideUpperBoundary returns whether the timestamp is outside the upper boundary time
// for chronological collection, this returns whether the time is AFTER the end time
// for reverse collection, this returns whether the time is BEFORE the start time
func (t *CollectionTimeRange) outsideUpperBoundary(timestamp time.Time) bool {

	if t.CollectionOrder == CollectionOrderChronological {
		return timestamp.After(t.To)
	}
	return timestamp.Before(t.From)
}

// onOrInsideLowerBoundary returns whether the timestamp is on the lower boundary time, or inside the lower boundary time
// for chronological collection, this returns whether the time is ON or AFTER the start time
// for reverse collection, this returns whether the time is ON or BEFORE the end time
func (t *CollectionTimeRange) onOrInsideLowerBoundary(timestamp time.Time) bool {

	return t.lowerBoundaryTime().Equal(timestamp) || t.insideLowerBoundary(timestamp)
}

//	onOrInsideUpperBoundary returns whether the timestamp is on the upper boundary time, or inside the upper boundary time
//
// for chronological collection, this returns whether the time is ON or BEFORE the end time
// for reverse collection, this returns whether the time is ON or AFTER the start time
func (t *CollectionTimeRange) onOrInsideUpperBoundary(timestamp time.Time) bool {

	return t.upperBoundaryTime().Equal(timestamp) || t.insideUpperBoundary(timestamp)
}

// setUpperBoundaryTime extends the upper boundary time of the collection range
// NOTE: we DO NOT set the upper boundary time if the new time is not extending the range
func (t *CollectionTimeRange) setUpperBoundaryTime(newTime time.Time) {

	if t.CollectionOrder == CollectionOrderChronological {
		// only set if the new time extends the range
		if newTime.After(t.To) {
			t.To = newTime
		}
	} else {
		// for reverse collection, only set if the new time extends the range (backwards in time)
		// the upper boundary for reverse collection is the From time
		// to extend the range, we need to set From to an earlier time
		if newTime.Before(t.From) {
			t.From = newTime
		}
	}
}

func (t *CollectionTimeRange) Validate() error {
	if t.From.IsZero() {
		return errors.New("from time is zero")
	}
	if t.To.IsZero() {
		return errors.New("to time is zero")
	}
	if t.From.After(t.To) {
		return errors.New("from time is after to time")
	}
	if t.CollectionOrder != CollectionOrderChronological && t.CollectionOrder != CollectionOrderReverse {
		return fmt.Errorf("invalid collection order: %v", t.CollectionOrder)
	}
	return nil
}

// RangesOverlap checks if this time range overlaps with another time range
// Two ranges overlap if the end of one is greater than or equal to the start of the other
// and the start of one is less than or equal to the end of the other
func (t *CollectionTimeRange) RangesOverlap(other *CollectionTimeRange) bool {
	return t.To.Compare(other.From) >= 0 && other.To.Compare(t.From) >= 0
}

// IsRangeSubsumed checks if this time range is completely contained within another time range
func (t *CollectionTimeRange) IsRangeSubsumed(other *CollectionTimeRange) bool {
	return t.From.Compare(other.From) >= 0 && t.To.Compare(other.To) <= 0
}
