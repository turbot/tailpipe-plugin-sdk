package formats

import (
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

type Regex struct {
	// the layout of the log line
	Layout string `hcl:"layout"`
}

func (c *Regex) Validate() error {
	return nil
}

func (c *Regex) Identifier() string {
	return constants.SourceFormatRegex
}
