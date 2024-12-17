package table

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
)

type CollectorWithFormat[R types.RowStruct, S parse.Config] struct {
	CollectorImpl[R]

	// the table format
	Format S
}

func (c *CollectorWithFormat[R, S]) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req
	// parse format config
	if err := c.initialiseFormat(req.SourceFormat); err != nil {
		return err
	}

	// set the format on the table (we know it supports it as our T TableWithFormat constraint ensures it)
	any(c.Table).(TableWithFormat[R, S]).SetFormat(c.Format)

	// now call base init
	return c.CollectorImpl.Init(ctx, req)
}

func (c *CollectorWithFormat[R, S]) initialiseFormat(formatData config_data.ConfigData) error {
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
		return fmt.Errorf("invalid partition config: %w", err)
	}

	return nil
}
