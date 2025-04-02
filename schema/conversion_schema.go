package schema

import (
	"github.com/turbot/go-kit/helpers"
	"golang.org/x/exp/maps"
	"sort"
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

// ConversionSchema is a specialised TableSchema which also contains a list of all source columns
type ConversionSchema struct {
	TableSchema
	// the source columns - these are the columns in the source data
	// this is to ensure we have the inputs required for any transforms
	SourceColumns []SourceColumnDef
}

// NewConversionSchemaWithInferredSchema populates a ConversionSchema schema using a table schema and an inferred row schema
// this is called from the CLI after receiving the first JSONL file
// it either adds all fields in the inferred schema (if AutoMapSourceFields is true) or
// just populate missing types if AutoMapSourceFields is false
func NewConversionSchemaWithInferredSchema(tableSchema, inferredSchema *TableSchema) *ConversionSchema {
	// initialize the conversion schema from the table schema def
	r := &ConversionSchema{
		TableSchema: *tableSchema,
	}

	var sourceColumns []SourceColumnDef

	//get the table schema as a map
	schemaMap := r.AsMap()
	excludedMap := helpers.SliceToLookup(r.ExcludeSourceFields)

	keys := maps.Keys(schemaMap)
	// ensure consistent order
	sort.Strings(keys)

	// First add all columns from the table schema
	for _, key := range keys {
		sourceColumns = append(sourceColumns, NewSourceColumnDef(schemaMap[key]))
	}

	// Then add any inferred columns that aren't already in the schema
	inferredSchemaMap := inferredSchema.AsMap()
	keys = maps.Keys(inferredSchemaMap)
	// ensure consistent order
	sort.Strings(keys)

	for _, key := range keys {
		// if this column exists in the table def, skip it
		inferredColumn, haveColumn := inferredSchemaMap[key]
		if haveColumn {
			continue
		}

		// if we are in autoMap mode, include column in TableSchema as long as it is not excluded
		if r.AutoMapSourceFields {
			// skip any excluded fields
			if _, excluded := excludedMap[inferredColumn.ColumnName]; excluded {
				continue
			}
			// we do not have this column - add it
			r.Columns = append(r.Columns, inferredColumn)
			sourceColumns = append(sourceColumns, NewSourceColumnDef(inferredColumn))
		}
	}

	// now set the source columns
	r.SourceColumns = sourceColumns
	return r
}

func NewConversionSchema(tableSchema *TableSchema) *ConversionSchema {
	// initialize the conversion schema from the table schema def
	r := &ConversionSchema{
		TableSchema: *tableSchema,
	}
	var sourceColumns []SourceColumnDef

	for _, c := range tableSchema.Columns {
		// store source columns
		sourceColumns = append(sourceColumns, NewSourceColumnDef(c))
	}

	r.SourceColumns = sourceColumns
	return r
}
