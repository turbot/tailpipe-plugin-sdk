package artifact_source

// AwsS3BucketSourceConfig is the configuration for an [AwsS3BucketSource]
type AwsS3BucketSourceConfig struct {
	SourceConfigBase
	Bucket     string   `hcl:"bucket"`
	Prefix     string   `hcl:"prefix"`
	Extensions []string `hcl:"extensions"`
	// TODO #config use Connection for credentials
	AccessKey    string `hcl:"access_key"`
	SecretKey    string `hcl:"secret_key"`
	SessionToken string `hcl:"session_token"`
}

func (a AwsS3BucketSourceConfig) Validate() error {
	//TODO #config validate the config
	return nil
}
