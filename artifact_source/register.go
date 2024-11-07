package artifact_source

import "github.com/turbot/tailpipe-plugin-sdk/row_source"

func RegisterSdkSources() {
	row_source.Factory.RegisterRowSources(NewAwsCloudWatchSource, NewAwsS3BucketSource, NewFileSystemSource, NewGcpStorageBucketSource)
}
