package formats

import (
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

type Regex struct {
	// the layout of the log line
	// NOTE that as will contain grok patterns, this property is included in constants.GrokConfigProperties
	// meaning and '{' will be auto-escaped in the hcl
	Layout string `hcl:"layout"`
}

func (c *Regex) Validate() error {
	return nil
}

func (c *Regex) Identifier() string {
	return constants.SourceFormatRegex
}
