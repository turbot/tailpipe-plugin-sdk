package formats

import (
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Grok struct {
	Name string `hcl:",label"`
	// the layout of the log line
	// NOTE that as will contain grok patterns, this property is included in constants.GrokConfigProperties
	// meaning and '{' will be auto-escaped in the hcl
	Layout string `hcl:"layout"`

	// grok patterns to add to the grok parser used to parse the layout
	Patterns map[string]string `hcl:"patterns,optional"`
}

func NewGrok() Format {
	return &Grok{}
}

func (c *Grok) Validate() error {
	return nil
}

// GetName returns the name of this format instance
func (c *Grok) GetName() string {
	return c.Name
}

// Identifier returns the format type identifier
func (c *Grok) Identifier() string {
	return constants.SourceFormatGrok
}

func (c *Grok) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	return mappers.NewGrokMapper[*types.DynamicRow](c.Layout, c.Patterns)
}
