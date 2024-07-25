package artifact

import "time"

// AwsCloudWatchSourceConfig is the configuration for an [AwsCloudWatchSource]
type AwsCloudWatchSourceConfig struct {
	SourceConfigBase
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
