package artifact_source

// GcpStorageBucketSourceConfig is the configuration for [GcpStorageBucketSource]
type GcpStorageBucketSourceConfig struct {
	Bucket     string   `hcl:"bucket"`
	Prefix     string   `hcl:"prefix"`
	Extensions []string `hcl:"extensions"`
	// TODO: Add additional fields
	// Project      *string // TODO: Revisit if we need this, doesn't seem to be settable on
	Credentials  *string `hcl:"credentials"`
	QuotaProject *string `hcl:"quota_project"`
	Impersonate  *string `hcl:"impersonate"`
}

func (g GcpStorageBucketSourceConfig) Validate() error {
	// TODO #config validate the config
	return nil
}
