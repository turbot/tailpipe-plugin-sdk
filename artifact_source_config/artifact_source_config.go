package artifact_source_config

import "github.com/turbot/tailpipe-plugin-sdk/parse"

type ArtifactSourceConfig interface {
	parse.Config

	GetFileLayout() *string
	GetJsonPath() *string
	DefaultTo(ArtifactSourceConfig)
}
