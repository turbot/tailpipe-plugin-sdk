package artifact_row_source

import (
	"time"
)

// AwsCloudWatchSourceConfig is the configuration for an [AwsCloudWatchSource]
type AwsCloudWatchSourceConfig struct {
	// TODO #confif connection based credentiuals mechanism
	AccessKey    string
	SecretKey    string
	SessionToken string

	// the log group to collect
	LogGroupName string

	// collect log streams with this prefixthe log stream prefix
	LogStreamPrefix *string

	// the time range to collect for
	StartTime time.Time
	EndTime   time.Time
}

func (a AwsCloudWatchSourceConfig) Validate() error {
	// TODO #config validate the config
	return nil
}
