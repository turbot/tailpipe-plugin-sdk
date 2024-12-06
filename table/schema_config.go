package table

import "github.com/turbot/tailpipe-plugin-sdk/schema"

// structs to represnt the schem in table config

type RowSchemaConfig struct {
	Columns []ColumnSchemaConfig `hcl:"columns,block"`
	// one of "full" (the default), "dynamic", "partial"
	// - in practice there is only any point in setting to partial, indicating this is not the full schema
	Mode schema.Mode `hcl:"modes"`
}

type ColumnSchemaConfig struct {
	Name string `hcl:"name,label"`
	Type string `hcl:"type"`
}
