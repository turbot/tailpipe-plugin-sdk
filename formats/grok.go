package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Grok struct {
	Name        string `hcl:",label"`
	Description string `hcl:"description,optional"`
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

func (g *Grok) Validate() error {
	return nil
}

// Identifier returns the format type identifier
func (g *Grok) Identifier() string {
	return constants.SourceFormatGrok
}

// GetName returns the name of this format instance
func (g *Grok) GetName() string {
	return g.Name
}

func (g *Grok) GetDescription() string {
	return g.Description
}

func (g *Grok) GetProperties() map[string]string {
	properties := make(map[string]string)

	properties["layout"] = g.Layout

	if g.Patterns != nil && len(g.Patterns) > 0 {
		for key, value := range g.Patterns {
			properties[fmt.Sprintf("pattern: %s", key)] = value
		}
	}

	return properties
}

func (g *Grok) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	return mappers.NewGrokMapper[*types.DynamicRow](g.Layout, g.Patterns)
}

func (g *Grok) GetRegex() (string, error) {
	mapper, err := mappers.NewGrokMapper[*types.DynamicRow](g.Layout, g.Patterns)
	if err != nil {
		return "", err
	}
	return mapper.GetRegex()
}
