package table

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
)

// CollectorWithFormat is a collector that has a table format
// The format is parsed from the source format config
type CollectorWithFormat[R types.RowStruct, S parse.Config] struct {
	CollectorImpl[R]

	// shadow the table field from the base collector, so we store it as a TableWithFormat,
	//to avoid the need for a type assertion
	Table TableWithFormat[R, S]
	// the table format
	Format S
}

func NewCollectorWithFormat[R types.RowStruct, S parse.Config, T TableWithFormat[R, S]]() *CollectorWithFormat[R, S] {
	table := utils.InstanceOf[T]()

	return &CollectorWithFormat[R, S]{
		Table: table,
		CollectorImpl: CollectorImpl[R]{
			Table: table,
		},
	}

}
func (c *CollectorWithFormat[R, S]) Init(ctx context.Context, req *types.CollectRequest) error {
	// parse format config
	if err := c.initialiseFormat(req.SourceFormat); err != nil {
		return err
	}

	// set the format on the table
	c.Table.SetFormat(c.Format)

	// now call base init
	return c.CollectorImpl.Init(ctx, req)
}

func (c *CollectorWithFormat[R, S]) initialiseFormat(formatData types.ConfigData) error {
	// default to empty format
	format := utils.InstanceOf[S]()
	if len(formatData.GetHcl()) > 0 {
		var err error
		format, err = parse.ParseConfig[S](formatData)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}

		slog.Info("CollectorImpl: format parsed", "format", c)
	}
	c.Format = format

	// validate format
	if err := format.Validate(); err != nil {
		return fmt.Errorf("invalid format config: %w", err)
	}

	return nil
}
