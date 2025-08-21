package schema

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
	"golang.org/x/exp/maps"
	"strings"
)

// SourceColumnDef is a simple struct to hold the column name and type for a source column
type SourceColumnDef struct {
	Name string

	Type string
}

func NewSourceColumnDef(columnSchema *ColumnSchema) SourceColumnDef {
	t := columnSchema.FullType()
	return SourceColumnDef{
		Name: columnSchema.SourceName,
		// use full type, i.e. expand struct types
		Type: t,
	}
}

// ConversionSchema is a specialised TableSchema which also contains a list of ALL source columns
// the embedded Schema defines the columns which appear in the output parquet file,
// and the source columns includes all available fields - these are necessary as there may be a transform which uses
// any of them
type ConversionSchema struct {
	TableSchema
	// the source columns - these are the columns in the source data
	// this is to ensure we have the inputs required for any transforms
	SourceColumns []SourceColumnDef

	ColumnString string
}

// NewConversionSchemaWithInferredSchema populates a ConversionSchema schema using a table schema and an inferred row schema
// this is called from the CLI after receiving the first JSONL file
// If a 'Select' pattern is provided, it will be used to select source fields to include in the schema
func NewConversionSchemaWithInferredSchema(tableSchema, inferredSchema *TableSchema) *ConversionSchema {
	// initialize the conversion schema from the table schema def
	r := &ConversionSchema{
		TableSchema: *tableSchema,
	}

	// build a list of source columns - these are the columns to read from the JSONL
	var sourceColumns = map[string]SourceColumnDef{}

	// First add the source column for all columns the table schema (unless there is transform)
	for _, c := range tableSchema.Columns {
		if c.Transform != "" {
			// skip this column - it is a transform so the source column will not be used
			continue
		}
		sourceColumns[c.SourceName] = NewSourceColumnDef(c)
	}

	// now populate the source columns from the inferred schema
	for _, c := range inferredSchema.Columns {
		// if we do not already have this column, add it
		if _, haveColumn := sourceColumns[c.SourceName]; haveColumn {
			continue
		}

		// add to source columns - we may use use any column for a transform
		sourceColumns[c.SourceName] = NewSourceColumnDef(c)

		// only add the inferred column to 'Columns' if it satisfies MapFields
		// does this column match the any of the map fields?
		if r.ShouldMapSourceColumn(c.ColumnName) {
			r.Columns = append(r.Columns, c)
		}
	}

	// now set the source columns
	r.SourceColumns = maps.Values(sourceColumns)

	// sort the columns alphabetically, with tp_ fields at the end
	r.initColumns()
	return r
}

func NewConversionSchema(tableSchema *TableSchema) *ConversionSchema {
	clonedSchema := tableSchema.Clone()
	// initialize the conversion schema from the table schema def
	r := &ConversionSchema{
		TableSchema: *clonedSchema,
	}
	var sourceColumns []SourceColumnDef

	// First add the source column for all columns the table schema (unless there is transform)
	for _, c := range tableSchema.Columns {
		if c.Transform != "" {
			// skip this column - it is a transform so the source column will not be used
			continue
		}
		sourceColumns = append(sourceColumns, NewSourceColumnDef(c))
	}

	// sort the columns alphabetically, with tp_ fields at the end
	r.SourceColumns = sourceColumns
	r.initColumns()

	return r
}

// initColumns sorts the columns in the ConversionSchema alphabetically, with tp_ prefixed fields at the end
// it also populates the ColumnString field with a comma-separated list of quoted column names
func (c *ConversionSchema) initColumns() {
	// sort the columns, alphabetically with tp fields at end

	// first put into a map
	columnMap := map[string]*ColumnSchema{}
	for _, c := range c.Columns {
		// store source columns
		columnMap[c.ColumnName] = c
	}
	sortedColumnNames := helpers.SortColumnsAlphabetically(maps.Keys(columnMap))

	columnNames := make([]string, len(c.SourceColumns))
	c.Columns = make([]*ColumnSchema, len(sortedColumnNames))
	for i, columnName := range sortedColumnNames {
		c.Columns[i] = columnMap[columnName]
		columnNames[i] = fmt.Sprintf(`"%s"`, columnName)
	}

	c.ColumnString = strings.Join(columnNames, ", ")

}
