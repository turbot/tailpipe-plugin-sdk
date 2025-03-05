package formats

import (
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Format interface {
	parse.Config

	GetName() string
	GetMapper() (mappers.Mapper[*types.DynamicRow], error)
	GetRegex() (string, error)
	GetDescription() string
	// GetProperties returns the format properties as a string map - used for introspection
	GetProperties() map[string]string
}
