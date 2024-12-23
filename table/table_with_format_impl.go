package table

import "github.com/turbot/tailpipe-plugin-sdk/parse"

// TableWithFormatImpl is a generic struct representing a plugin table definition with a format
type TableWithFormatImpl[S parse.Config] struct {
	Format S
}

func (c *TableWithFormatImpl[S]) SetFormat(format S) {
	c.Format = format
}
