package artifact_source

// AwsS3BucketSourceConfig is the configuration for an [AwsS3BucketSource]
type AwsS3BucketSourceConfig struct {
	SourceConfigBase
	Bucket     string
	Prefix     string
	Extensions []string
	// TODO #config use Connection for credentials
	AccessKey    string
	SecretKey    string
	SessionToken string
}

func (a AwsS3BucketSourceConfig) Validate() error {
	//TODO #config validate the config
	return nil
}
