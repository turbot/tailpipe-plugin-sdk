package table

import "github.com/turbot/tailpipe-plugin-sdk/schema"

// structs to represnt the schem in table config

type RowSchemaConfig struct {
	Columns []ColumnSchemaConfig `hcl:"columns,block"`
	// one of "full" (the default), "dynamic", "partial"
	// - in practice there is only any point in setting to partial, indicating this is not the full schema
	Mode schema.Mode `hcl:"modes"`
}

func (c RowSchemaConfig) ToRowSchema() *schema.RowSchema {
	var res = &schema.RowSchema{
		Mode: c.Mode,
	}
	for _, col := range c.Columns {
		res.Columns = append(res.Columns, &schema.ColumnSchema{
			// source name and column name are the same in this case
			SourceName: col.Name,
			ColumnName: col.Name,
			Type:       col.Type,
		})
	}
	return res
}

type ColumnSchemaConfig struct {
	Name string `hcl:"name,label"`
	Type string `hcl:"type"`
}
