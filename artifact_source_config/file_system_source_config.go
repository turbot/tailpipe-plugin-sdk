package artifact_source_config

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/pipe-fittings/utils"
)

const (
	FileSystemSourceIdentifier = "file_system"
)

type FileSystemSourceConfig struct {
	ArtifactSourceConfigBase
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	Paths []string `hcl:"paths"`
}

func (f *FileSystemSourceConfig) Validate() error {
	// validate the base fields
	if err := f.ArtifactSourceConfigBase.Validate(); err != nil {
		return err
	}

	// validate we have at least one path
	if len(f.Paths) == 0 {
		return fmt.Errorf("required field: paths can not be empty")
	}

	// validate paths exist on the file system
	for _, path := range f.Paths {
		if !utils.IsValidDir(path) {
			return fmt.Errorf("path %s is not a directory or does not exist", path)
		}
	}

	return nil
}

func (f *FileSystemSourceConfig) Identifier() string {
	return FileSystemSourceIdentifier
}
