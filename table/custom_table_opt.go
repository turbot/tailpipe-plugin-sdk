package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// CustomTableOpt is a function that can be used to set options on a custom table
type CustomTableOpt = func(c CustomTable)

func WithTableDef(tableDef *types.CustomTableDef, format parse.Config) CustomTableOpt {
	return func(t CustomTable) {

		t.SetTableDef(tableDef)
		t.SetFormat(format)
	}
}
