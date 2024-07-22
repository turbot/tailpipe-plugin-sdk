package paging

import (
	"fmt"
	"maps"
)

// Cloudwatch contains paging data for the Cloudwatch artifact source
// This contains the latest timestamp fetched for each log stream in a SINGLE log group
type Cloudwatch struct {
	// The timestamp of the last collected log for each log stream
	// expressed as the number of milliseconds after Jan 1, 1970 00:00:00 UTC.
	Timestamps map[string]int64 `json:"timestamp"`
}

// Update updates the Cloudwatch paging data with the latest data
func (c *Cloudwatch) Update(data Data) error {
	other, ok := data.(*Cloudwatch)
	if !ok {
		return fmt.Errorf("cannot update Cloudwatch paging data with %T", data)
	}
	// merge the timestamps, preferring the latest
	maps.Copy(c.Timestamps, other.Timestamps)
	return nil
}

func NewCloudwatch() *Cloudwatch {
	return &Cloudwatch{
		Timestamps: make(map[string]int64),
	}
}
