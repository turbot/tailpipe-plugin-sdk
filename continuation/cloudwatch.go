package continuation

// Cloudwatch contains continuation data for the Cloudwatch artifact source
type Cloudwatch struct {
	// The timestamp of the last collected log
	// expressed as the number of milliseconds after Jan 1, 1970 00:00:00 UTC.
	Timestamp int64 `json:"timestamp"`
}

// marker interface
func (Cloudwatch) isContinuationData() {}
