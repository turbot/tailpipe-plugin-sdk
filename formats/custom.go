package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Custom struct {
	// the layout of the log line
	// NOTE that as will contain grok patterns, this property is included in constants.GrokConfigProperties
	// meaning and '{' will be auto-escaped in the hcl
	Layout string `hcl:"layout"`

	// grok patterns to add to the grok parser used to parse the layout
	Patterns map[string]string `hcl:"patterns,optional"`

	// the roq schema must at the minimum provide mapping for the tp_timestamp field
	//Schema *schema.RowSchema `hcl:"schema,block"`
}

func (c *Custom) Validate() error {
	return nil
}

func (c *Custom) Identifier() string {
	return constants.SourceFormatCustom
}

//func (c *Custom) GetSchema() *schema.RowSchema {
//	//if c.Schema == nil {
//	//	return nil
//	//}
//	//
//	//return c.Schema.ToRowSchema()
//	return nil
//}

func NewCustomFormat(formatData *types.FormatConfigData) (*Custom, error) {
	if len(formatData.GetHcl()) > 0 {
		var err error
		format, err := parse.ParseConfig[*Custom](formatData)
		if err != nil {
			return nil, fmt.Errorf("error parsing config: %w", err)
		}

		return format, nil
	}
	return &Custom{}, nil
}
