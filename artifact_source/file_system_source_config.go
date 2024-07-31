package artifact_source

type FileSystemSourceConfig struct {
	Paths      []string
	Extensions []string
}

func (f FileSystemSourceConfig) Validate() error {
	//TODO #config  implement me
	return nil
}
