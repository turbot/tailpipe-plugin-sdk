package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// CustomTableOpt is a function that can be used to set options on a custom table
type CustomTableOpt = func(c CustomTable)

func WithTableDef(customTableSchema *schema.TableSchema, format parse.Config) CustomTableOpt {
	return func(t CustomTable) {
		t.Initialize(format, customTableSchema)
	}
}
