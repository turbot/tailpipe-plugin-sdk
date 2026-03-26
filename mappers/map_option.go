package mappers

type MapOption[R any] func(Mapper[R])

// WithHeader is a MapOption that allows you to set the header for the mapper
func WithHeader[R any](header []string) MapOption[R] {
	return func(m Mapper[R]) {
		if h, ok := m.(HeaderHandler); ok {
			h.OnHeader(header)
		}
	}
}
