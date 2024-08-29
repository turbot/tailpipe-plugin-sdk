package artifact_source

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
)

type ArtifactConfig interface {
	parse.Config

	GetFileLayout() *string
	GetJsonPath() *string
	DefaultTo(ArtifactConfig)
}

type ArtifactSourceConfigBase struct {
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	// regex string defining the file name pattern, with named groups to extract properties
	FileLayout *string `hcl:"file_layout,optional"`

	// the json path to extract the properties from the data
	JsonPath *string `hcl:"json_path,optional"`
}

func (b *ArtifactSourceConfigBase) Validate() error {
	// TODO #config #valiate
	return nil
}

func (b *ArtifactSourceConfigBase) GetFileLayout() *string {
	return b.FileLayout
}

func (b *ArtifactSourceConfigBase) GetJsonPath() *string {
	return b.JsonPath
}

func (b *ArtifactSourceConfigBase) DefaultTo(other ArtifactConfig) {
	if helpers.IsNil(other) {
		return
	}

	if other.GetFileLayout() != nil && b.FileLayout == nil {
		b.FileLayout = other.GetFileLayout()
	}
	if other.GetJsonPath() != nil && b.JsonPath == nil {
		b.JsonPath = other.GetJsonPath()
	}
}
