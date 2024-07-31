package artifact_source

// GcpStorageBucketSourceConfig is the configuration for [GcpStorageBucketSource]
type GcpStorageBucketSourceConfig struct {
	Bucket     string
	Prefix     string
	Extensions []string
	// TODO: Add additional fields
	// Project      *string // TODO: Revisit if we need this, doesn't seem to be settable on
	Credentials  *string
	QuotaProject *string
	Impersonate  *string
}

func (g GcpStorageBucketSourceConfig) Validate() error {
	// TODO #config validate the config
	return nil
}
