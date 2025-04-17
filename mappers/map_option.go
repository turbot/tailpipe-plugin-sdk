package mappers

import (
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type MapOption[R types.RowStruct] func(Mapper[R])

// WithHeader is a MapOption that allows you to set the header for the mapper
func WithHeader[R types.RowStruct](header []string) MapOption[R] {
	return func(m Mapper[R]) {
		if h, ok := m.(HeaderHandler); ok {
			h.OnHeader(header)
		}
	}
}
