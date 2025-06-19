package collection_state

import "time"

type TimeRange struct {
	From time.Time
	To   time.Time
}

// Contains checks if the given timestamp is within the time range.
// The logic differs depending on the collection order:
// For chronological order, the from time is inclusive and the to time is exclusive
// For reverse order, the from time is exclusive and the to time is inclusive
func (r TimeRange) Contains(timestamp time.Time, order CollectionOrder) bool {
	if order == CollectionOrderChronological {
		return timestamp.Compare(r.From) >= 0 && timestamp.Compare(r.To) < 0
	}
	return timestamp.Compare(r.From) < 0 && timestamp.Compare(r.To) >= 0
}

// upperBoundaryTime returns the the furthest time in the direction of collection
// i.e. if we are collecting forwards, the upperBoundaryTime is the end time of the range,
// if we are collecting backwards, the upperBoundaryTime is the start time of the range
func (r TimeRange) upperBoundaryTime(order CollectionOrder) time.Time {
	if order == CollectionOrderChronological {
		return r.To
	}
	return r.From
}

// lowerBoundaryTime returns the the furthest time in the opposite direction of collection
// i.e. if we are collecting forwards, the lowerBoundaryTime is the start time of the range,
// if we are collecting backwards, the lowerBoundaryTime is the end time of the range
func (r TimeRange) lowerBoundaryTime(order CollectionOrder) time.Time {
	if order == CollectionOrderChronological {
		return r.From
	}
	return r.To
}
