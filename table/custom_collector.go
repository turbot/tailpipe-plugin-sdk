package table

import (
	"log/slog"

	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// CustomCollector is a collector that has a table format
// The format is parsed from the source format config
type CustomCollector struct {
	CollectorImpl[*DynamicRow]
	// shadow the table field from the base collector, so we store it as a CustomTable,
	//to avoid the need for a type assertion
	Table     CustomTable
	tableName string
}

// CustomTableOpt is a function that can be used to set options on a custom table
// note: as type assertion is done inside the option, there is no need to
// accept a generic type for the option func, simplifying the declaration
type CustomTableOpt = func(c any)

func WithTableDef(tableDef *types.CustomTableDef, format parse.Config) CustomTableOpt {
	return func(t any) {
		// if the table supports setting schema, set it
		type SchemaSetter interface {
			SetFormat(parse.Config)
			SetCustomTableDef(*types.CustomTableDef)
		}
		if ss, ok := any(t).(SchemaSetter); ok {
			ss.SetCustomTableDef(tableDef)
			ss.SetFormat(format)
		}
	}
}

func NewCustomCollector[T CustomTable](t T) *CustomCollector {
	slog.Info("Creating new custom collector")

	c := &CustomCollector{
		Table: t,
		CollectorImpl: CollectorImpl[*DynamicRow]{
			Table: t,
		},
	}

	return c
}
