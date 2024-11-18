package parse

// Config is an interface that all configuration structs must implement - this includes:
// - table config
// - source config
// - connection config
type Config interface {
	Validate() error
	Identifier() string
}
