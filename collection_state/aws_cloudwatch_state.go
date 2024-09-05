package collection_state

import "github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"

// AwsCloudwatchState contains collection state data for the AwsCloudwatchState artifact source
// This contains the latest timestamp fetched for each log stream in a SINGLE log group
type AwsCloudwatchState struct {
	CollectionStateBase
	// The timestamp of the last collected log for each log stream
	// expressed as the number of milliseconds after Jan 1, 1970 00:00:00 UTC.
	Timestamps map[string]int64 `json:"timestamps"`
}

func NewAwsCloudwatchCollectionState() CollectionState[*artifact_source_config.AwsCloudWatchSourceConfig] {
	return &AwsCloudwatchState{
		Timestamps: make(map[string]int64),
	}
}

func (s *AwsCloudwatchState) Init(*artifact_source_config.AwsCloudWatchSourceConfig) error {
	return nil
}

// Upsert adds new/updates an existing logstream  with its current timestamp
func (s *AwsCloudwatchState) Upsert(name string, time int64) {
	s.Mut.Lock()
	defer s.Mut.Unlock()

	if s.Timestamps == nil {
		s.Timestamps = make(map[string]int64)
	}
	if time == 0 {
		return
	}
	s.Timestamps[name] = time
}

func (s *AwsCloudwatchState) IsEmpty() bool {
	return len(s.Timestamps) == 0
}
