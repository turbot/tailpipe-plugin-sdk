package artifact

// AwsS3BucketSourceConfig is the configuration for an [AwsS3BucketSource]
type AwsS3BucketSourceConfig struct {
	Bucket       string
	Prefix       string
	Extensions   []string
	AccessKey    string
	SecretKey    string
	SessionToken string
}
