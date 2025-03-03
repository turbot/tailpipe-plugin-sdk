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

func (r *Regex) Validate() error {
	return nil
}

// Identifier returns the format type identifier
func (r *Regex) Identifier() string {
	return constants.SourceFormatRegex
}

// GetName returns the name of this format instance
func (r *Regex) GetName() string {
	return r.Name
}

func (r *Regex) GetDescription() string {
	return r.Description
}

func (r *Regex) GetRegex() (string, error) {
	return r.Layout, nil
}

func (r *Regex) GetProperties() map[string]string {
	return map[string]string{
		"layout": r.Layout,
	}
}

func (r *Regex) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	return mappers.NewRegexMapper[*types.DynamicRow](r.Layout)
}
