package artifact_source

type FileSystemSourceConfig struct {
	Paths      []string `hcl:"paths"`
	Extensions []string `hcl:"extensions"`
}

func (f *FileSystemSourceConfig) Validate() error {
	//TODO #config  implement me https://github.com/turbot/tailpipe-plugin-sdk/issues/12
	return nil
}
