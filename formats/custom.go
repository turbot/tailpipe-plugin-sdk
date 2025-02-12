package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-core/formats"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
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

func NewCustomFormat(T, error) interface{} {
	if len(formatData.GetHcl()) > 0 {
		var err error
		format, err := parse.ParseConfig[*Custom](formatData)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}

		slog.Info("CollectorImpl: format parsed", "format", c)
	}

}
