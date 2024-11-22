package artifact_source_config

import (
	"fmt"
	"time"

	"github.com/hashicorp/hcl/v2"
)

// AwsCloudWatchSourceConfig is the configuration for an [AwsCloudWatchSource]
type AwsCloudWatchSourceConfig struct {
	ArtifactSourceConfigBase
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	// the log group to collect
	LogGroupName string `hcl:"log_group_name"`
	// collect log streams with this prefixthe log stream prefix
	LogStreamPrefix *string `hcl:"log_stream_prefix"`
	// the time range to collect for
	StartTimeString string `hcl:"start_time"`
	EndTimeString   string `hcl:"end_time"`
	StartTime       time.Time
	EndTime         time.Time
}

func (a AwsCloudWatchSourceConfig) Validate() error {
	// parse  start  and end time
	if a.StartTimeString == "" {
		return fmt.Errorf("start_time is required")
	}
	startTime, err := time.Parse(time.RFC3339, a.StartTimeString)
	if err != nil {
		return fmt.Errorf("invalid start_time: %v", err)
	}
	a.StartTime = startTime
	if a.EndTimeString == "" {
		return fmt.Errorf("end_time is required")

	}
	endTime, err := time.Parse(time.RFC3339, a.EndTimeString)
	if err != nil {
		return fmt.Errorf("invalid end_time: %v", err)
	}
	a.EndTime = endTime
	if a.StartTime.After(a.EndTime) {
		return fmt.Errorf("start_time must be before end_time")
	}

	return a.ArtifactSourceConfigBase.Validate()
}

func (AwsCloudWatchSourceConfig) Identifier() string {
	return AWSCloudwatchSourceIdentifier
}
