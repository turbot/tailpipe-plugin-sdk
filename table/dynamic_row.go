package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DynamicRow struct {
	enrichment.CommonFields

	// dynamic columns
	Columns map[string]any

	Schema *schema.RowSchema
}

func NewDynamicRow() *DynamicRow {
	return &DynamicRow{
		Columns: make(map[string]any),
	}
}

// InitialiseFromMap initializes the struct from a map of string values
func (l *DynamicRow) InitialiseFromMap(m map[string]string) error {
	// TODO if we have a schema apply it to convert the column types
	// for now we just store the values as strings
	for k, v := range m {
		l.Columns[k] = v
	}
	return nil
}
