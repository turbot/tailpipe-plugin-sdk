package artifact_source

type GcpConnection struct {
}

func (c GcpConnection) Validate() error {
	return nil
}

func (GcpConnection) Identifier() string {
	return "gcp"
}
