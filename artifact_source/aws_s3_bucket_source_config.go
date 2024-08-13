package artifact_source

// AwsS3BucketSourceConfig is the configuration for an [AwsS3BucketSource]
type AwsS3BucketSourceConfig struct {
	Bucket        string   `hcl:"bucket"`
	Prefix        string   `hcl:"prefix"`
	Extensions    []string `hcl:"extensions"`
	Region        *string  `hcl:"region"`
	StartAfterKey *string  `hcl:"start_after_key"`

	// TODO #config use Connection for credentials https://github.com/turbot/tailpipe-plugin-sdk/issues/8
	AccessKey    string `hcl:"access_key"`
	SecretKey    string `hcl:"secret_key"`
	SessionToken string `hcl:"session_token"`
}

func (a AwsS3BucketSourceConfig) Validate() error {
	//TODO #config validate the config https://github.com/turbot/tailpipe-plugin-sdk/issues/9
	return nil
}
