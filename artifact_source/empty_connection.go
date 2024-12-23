package artifact_source

// for now, sources must be parametrized by a connection, so we need a dummy connection for those that don't need one
type EmptyConnection struct {
}

func (c *EmptyConnection) Validate() error {
	return nil
}

func (EmptyConnection) Identifier() string {
	return "empty"
}
