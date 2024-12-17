package table

import "github.com/turbot/tailpipe-plugin-sdk/parse"

type TableWithFormatImpl[S parse.Config] struct {
	Format S
}

func (c *TableWithFormatImpl[S]) SetFormat(format S) {
	c.Format = format
}
