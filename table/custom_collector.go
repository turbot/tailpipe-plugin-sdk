package table

import (
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
)

// CustomCollector is a collector that has a table format
// The format is parsed from the source format config
type CustomCollector struct {
	CollectorImpl[*DynamicRow]
	// shadow the table field from the base collector, so we store it as a CustomTable,
	//to avoid the need for a type assertion
	Table CustomTable
	// the table format
	Format *formats.Custom
}

func NewCustomCollector[T CustomTable](format *formats.Custom) *CustomCollector {
	t := utils.InstanceOf[T]()
	// set the format on the table
	t.SetFormat(format)

	return &CustomCollector{
		Table: t,
		CollectorImpl: CollectorImpl[*DynamicRow]{
			Table: t,
		},
		Format: format,

	}

}
//func (c *CustomCollector) Init(ctx context.Context, req *types.CollectRequest) error {
//	// TODO only required if we can override the format
//	// parse format config
//	if err := c.initialiseFormat(req.SourceFormat); err != nil {
//		return err
//	}
//
//
//	// now call base init
//	return c.CollectorImpl.Init(ctx, req)
//}
//
//func (c *CustomCollector) initialiseFormat(formatData types.ConfigData) error {
//	// default to empty format
//	format := &formats.Custom{}
//	if len(formatData.GetHcl()) > 0 {
//		var err error
//		format, err = parse.ParseConfig[*formats.Custom](formatData)
//		if err != nil {
//			return fmt.Errorf("error parsing config: %w", err)
//		}
//
//		slog.Info("CollectorImpl: format parsed", "format", c)
//	}
//	c.Format = format
//
//	// validate format
//	if err := format.Validate(); err != nil {
//		return fmt.Errorf("invalid format config: %w", err)
//	}
//
//	return nil
//}
