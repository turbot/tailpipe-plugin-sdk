package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
)

type DynamicRow struct {
	enrichment.CommonFields

	// dynamic columns
	Columns map[string]any

	// TODO do we need to convert
	//Schema *schema.RowSchema
}

func NewDynamicRow() *DynamicRow {
	return &DynamicRow{
		Columns: make(map[string]any),
	}
}

// InitialiseFromMap initializes the struct from a map of string values
func (l *DynamicRow) InitialiseFromMap(m map[string]string) error {
	// TODO if we have a schema apply it to convert the column types - is this needed

	// for now we just store the values as strings
	for k, v := range m {
		l.Columns[k] = v
	}
	return nil
}
