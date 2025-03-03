package formats

import (
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Regex struct {
	Name        string `hcl:",label"`
	Description string `hcl:"description,optional"`
	// the layout of the log line
	// NOTE that as will contain grok patterns, this property is included in constants.GrokConfigProperties
	// meaning and '{' will be auto-escaped in the hcl
	Layout string `hcl:"layout"`
}

func NewRegex() Format {
	return &Regex{}
}

func (c *Regex) Validate() error {
	return nil
}

// Identifier returns the format type identifier
func (c *Regex) Identifier() string {
	return constants.SourceFormatRegex
}

// GetName returns the name of this format instance
func (c *Regex) GetName() string {
	return c.Name
}

func (c *Regex) GetDescription() string {
	return c.Description
}

func (c *Regex) GetRegex() (string, error) {
	return c.Layout, nil
}

func (c *Regex) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	return mappers.NewRegexMapper[*types.DynamicRow](c.Layout)
}
