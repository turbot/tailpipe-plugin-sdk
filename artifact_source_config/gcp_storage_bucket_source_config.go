package artifact_source_config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
)

// GcpStorageBucketSourceConfig is the configuration for [GcpStorageBucketSource]
type GcpStorageBucketSourceConfig struct {
	ArtifactSourceConfigBase
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	Bucket     string   `hcl:"bucket"`
	Prefix     string   `hcl:"prefix"`
	Extensions []string `hcl:"extensions"`
	// TODO: Add additional fields https://github.com/turbot/tailpipe-plugin-sdk/issues/15
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

func (*GcpStorageBucketSourceConfig) Identifier() string {
	return GcpStorageBucketSourceIdentifier
}
