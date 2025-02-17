package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Grok struct {
	// the layout of the log line
	// NOTE that as will contain grok patterns, this property is included in constants.GrokConfigProperties
	// meaning and '{' will be auto-escaped in the hcl
	Layout string `hcl:"layout"`

	// grok patterns to add to the grok parser used to parse the layout
	Patterns map[string]string `hcl:"patterns,optional"`
}

func (c *Grok) Validate() error {
	return nil
}

func (c *Grok) Identifier() string {
	return constants.SourceFormatGrok
}

func NewCustomFormat(formatData *types.FormatConfigData) (*Grok, error) {
	if len(formatData.GetHcl()) > 0 {
		var err error
		format, err := parse.ParseConfig[*Grok](formatData)
		if err != nil {
			return nil, fmt.Errorf("error parsing config: %w", err)
		}

		return format, nil
	}
	return &Grok{}, nil
}
