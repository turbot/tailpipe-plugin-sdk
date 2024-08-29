package artifact_source

import "github.com/hashicorp/hcl/v2"

// AwsS3BucketSourceConfig is the configuration for an [AwsS3BucketSource]
type AwsS3BucketSourceConfig struct {
	ArtifactSourceConfigBase
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	Bucket     string   `hcl:"bucket"`
	Prefix     string   `hcl:"prefix"`
	Extensions []string `hcl:"extensions"`
	Region     *string  `hcl:"region"`
	// TODO #config change to period, then calculate the time/key
	StartAfterKey *string `hcl:"start_after_key"`

	// TODO #config use Connection for credentials https://github.com/turbot/tailpipe-plugin-sdk/issues/8
	AccessKey    string `hcl:"access_key"`
	SecretKey    string `hcl:"secret_key"`
	SessionToken string `hcl:"session_token,optional"`
}

func (c AwsS3BucketSourceConfig) Validate() error {
	//TODO #config validate the config https://github.com/turbot/tailpipe-plugin-sdk/issues/9
	return nil
}
