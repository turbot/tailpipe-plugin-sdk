package formats

import (
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

type Custom struct {
	// the layout of the log line
	// NOTE that as will contain grok patterns, this property is included in constants.GrokConfigProperties
	// meaning and '{' will be auto-escaped in the hcl
	Layout string

	// grok patterns to add to the grok parser used to parse the layout
	Patterns map[string]string

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
