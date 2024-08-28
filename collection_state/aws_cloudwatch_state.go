package collection_state

// AwsCloudwatchState contains collection state data for the AwsCloudwatchState artifact source
// This contains the latest timestamp fetched for each log stream in a SINGLE log group
type AwsCloudwatchState struct {
	CollectionStateBase
	// The timestamp of the last collected log for each log stream
	// expressed as the number of milliseconds after Jan 1, 1970 00:00:00 UTC.
	Timestamps map[string]int64 `json:"timestamps"`
}

func NewAwsCloudwatch() *AwsCloudwatchState {
	return &AwsCloudwatchState{
		Timestamps: make(map[string]int64),
	}
}

// Upsert adds new/updates an existing logstream  with its current timestamp
func (c *AwsCloudwatchState) Upsert(name string, time int64) {
	c.Mut.Lock()
	defer c.Mut.Unlock()

	if c.Timestamps == nil {
		c.Timestamps = make(map[string]int64)
	}
	if time == 0 {
		return
	}
	c.Timestamps[name] = time
}

func (b *AwsCloudwatchState) IsEmpty() bool {
	return len(b.Timestamps) == 0
}
