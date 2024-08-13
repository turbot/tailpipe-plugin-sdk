package paging

import "sync"

// Cloudwatch contains paging data for the Cloudwatch artifact source
// This contains the latest timestamp fetched for each log stream in a SINGLE log group
type Cloudwatch struct {
	// The timestamp of the last collected log for each log stream
	// expressed as the number of milliseconds after Jan 1, 1970 00:00:00 UTC.
	Timestamps    map[string]int64 `json:"timestamps"`
	timestampLock sync.Mutex
}

func NewCloudwatch() *Cloudwatch {
	return &Cloudwatch{
		Timestamps: make(map[string]int64),
	}
}

// Upsert adds new/updates an existing logstream  with its current timestamp
func (c *Cloudwatch) Upsert(name string, time int64) {
	c.timestampLock.Lock()
	defer c.timestampLock.Unlock()

	if c.Timestamps == nil {
		c.Timestamps = make(map[string]int64)
	}
	if time == 0 {
		return
	}
	c.Timestamps[name] = time
}

// implement marker interface
func (*Cloudwatch) pagingData() {}
