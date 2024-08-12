package artifact_source

import (
	"errors"
	"fmt"
	"strings"
)

// GcpStorageBucketSourceConfig is the configuration for [GcpStorageBucketSource]
type GcpStorageBucketSourceConfig struct {
	Bucket     string   `hcl:"bucket"`
	Prefix     string   `hcl:"prefix"`
	Extensions []string `hcl:"extensions"`
	// TODO: Add additional fields https://github.com/turbot/tailpipe-plugin-sdk/issues/15
	// Project      *string // TODO: Revisit if we need this, doesn't seem to be settable on https://github.com/turbot/tailpipe-plugin-sdk/issues/15
	Credentials  *string `hcl:"credentials"`
	QuotaProject *string `hcl:"quota_project"`
	Impersonate  *string `hcl:"impersonate"`
}

func (g *GcpStorageBucketSourceConfig) Validate() error {
	if g.Bucket == "" {
		return errors.New("bucket is required")
	}

	// Check format of extensions
	var invalidExtensions []string
	for _, e := range g.Extensions {
		if len(e) == 0 {
			invalidExtensions = append(invalidExtensions, "<empty>")
		} else if e[0] != '.' {
			invalidExtensions = append(invalidExtensions, e)
		}
	}
	if len(invalidExtensions) > 0 {
		return fmt.Errorf("invalid extensions: %s", strings.Join(invalidExtensions, ","))
	}

	return nil
}
