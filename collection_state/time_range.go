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
