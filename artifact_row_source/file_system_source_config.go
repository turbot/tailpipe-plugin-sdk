package artifact_row_source

type FileSystemSourceConfig struct {
	Paths      []string `hcl:"paths"`
	Extensions []string `hcl:"extensions"`
}

func (f FileSystemSourceConfig) Validate() error {
	//TODO #config  implement me
	return nil
}
