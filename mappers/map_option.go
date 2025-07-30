package mappers

type MapConfig struct {
	// for delimited files with a header, this will contain the header
	// (set by the WithHeader option)
	Header []string
}
type MapOption func(*MapConfig)

// WithHeader is a MapOption that allows you to set the header for the mapper
func WithHeader(header []string) MapOption {
	return func(c *MapConfig) {
		c.Header = header
	}
}
