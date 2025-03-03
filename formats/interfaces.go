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
	// GetFormatString returns the format as a string which can be included in the introspection response
	// (we need this as different formats may have different configuration options - we need a common way to represent them)
	GetFormatString() string
	// GetDescription returns an (optional) description of the format
	GetDescription() string

}
