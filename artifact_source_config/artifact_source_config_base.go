package artifact_source_config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/go-kit/helpers"
)

type ArtifactSourceConfigBase struct {
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	// regex string defining the file name pattern, with named groups to extract properties
	FileLayout *string `hcl:"file_layout,optional"`
}

func (b *ArtifactSourceConfigBase) Validate() error {
	return nil
}

func (b *ArtifactSourceConfigBase) Identifier() string {
	return "artifact_source"
}

func (b *ArtifactSourceConfigBase) GetFileLayout() *string {
	return b.FileLayout
}

func (b *ArtifactSourceConfigBase) DefaultTo(other ArtifactSourceConfig) {
	if helpers.IsNil(other) {
		return
	}

	if other.GetFileLayout() != nil && b.FileLayout == nil {
		b.FileLayout = other.GetFileLayout()
	}
}
