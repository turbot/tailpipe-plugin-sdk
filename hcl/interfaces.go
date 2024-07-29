package hcl

type Config interface {
	Validate() error
}
