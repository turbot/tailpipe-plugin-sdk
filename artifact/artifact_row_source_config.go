package artifact

type ArtifactRowSourceConfig struct {
	Source string `hcl:"source"`
}

func (c ArtifactRowSourceConfig) Validate() error {
	return nil
}
