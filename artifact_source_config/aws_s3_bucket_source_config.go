package artifact_source_config

import (
	"github.com/hashicorp/hcl/v2"
)

// AwsS3BucketSourceConfig is the configuration for an [AwsS3BucketSource]
type AwsS3BucketSourceConfig struct {
	ArtifactSourceConfigBase
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	Bucket        string   `hcl:"bucket"`
	Prefix        string   `hcl:"prefix"`
	Extensions    []string `hcl:"extensions"`
	Region        *string  `hcl:"region"`
	StartAfterKey *string  `hcl:"start_after_key"`

	// TODO #config use Connection for credentials https://github.com/turbot/tailpipe-plugin-sdk/issues/8
	AccessKey    string `hcl:"access_key"`
	SecretKey    string `hcl:"secret_key"`
	SessionToken string `hcl:"session_token,optional"`

	// TODO #config better naming
	LexicographicalOrder bool `hcl:"lexicographical_order,optional"`
}

func (c AwsS3BucketSourceConfig) Validate() error {
	//TODO #config validate the config https://github.com/turbot/tailpipe-plugin-sdk/issues/9
	return nil
}

func (AwsS3BucketSourceConfig) Identifier() string {
	return AwsS3BucketSourceIdentifier
}
