package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
)

// empty config implementation used for  PluginSourceWrapper
// as we need to set connection and config type
// NOTE: the config and connection will not be used
// - instead source in the remote plugin will instantiate the appropriate connection and config type

type NilConfig struct{}

func (n NilConfig) Validate() error {
	return nil
}

func (n NilConfig) Identifier() string {
	return "empty_config"
}

type NilArtifactSourceConfig struct{}

func (n NilArtifactSourceConfig) Validate() error {
	return nil
}

func (n NilArtifactSourceConfig) Identifier() string {
	return "empty_artifact_source_config"
}

func (n NilArtifactSourceConfig) GetFileLayout() *string {
	return nil
}

func (n NilArtifactSourceConfig) DefaultTo(_ artifact_source_config.ArtifactSourceConfig) {
}
