package parse

type Config interface {
	Validate() error
}
