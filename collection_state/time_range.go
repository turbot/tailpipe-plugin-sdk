package collection_state

import "time"

type timeRange struct {
	from time.Time
	to   time.Time
}

// Contains checks if the given timestamp is within the time range.
// i.e. timestamp >= from and timestamp < to
func (r timeRange) Contains(timestamp time.Time) bool {
	return timestamp.Compare(r.from) >= 0 && timestamp.Before(r.to)
}

// upperBoundaryTime returns the the furthest time in the direction of collection
// i.e. if we are collecting forwards, the upperBoundaryTime is the end time of the range,
// if we are collecting backwards, the upperBoundaryTime is the start time of the range
func (r timeRange) upperBoundaryTime(order CollectionOrder) time.Time {
	if order == CollectionOrderChronological {
		return r.to
	}
	return r.from
}

// lowerBoundaryTime returns the the furthest time in the opposite direction of collection
// i.e. if we are collecting forwards, the lowerBoundaryTime is the start time of the range,
// if we are collecting backwards, the lowerBoundaryTime is the end time of the range
func (r timeRange) lowerBoundaryTime(order CollectionOrder) time.Time {
	if order == CollectionOrderChronological {
		return r.from
	}
	return r.to
}
