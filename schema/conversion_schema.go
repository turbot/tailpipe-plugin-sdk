package schema

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
	var sourceColumns []SourceColumnDef

	//get the table schema as a map
	schemaMap := r.AsMap()

	// First add the source column for all columns the table schema (unless there is transform)
	for _, c := range tableSchema.Columns {
		if c.Transform != "" {
			// skip this column - it is a transform so the source column will not be used
			continue
		}
		sourceColumns = append(sourceColumns, NewSourceColumnDef(c))
	}

	// now populate the source columns from the inferred schema
	for _, c := range inferredSchema.Columns {
		// if this column exists in the table def, we have already added it to source columns so nothing to do
		if _, haveColumn := schemaMap[c.ColumnName]; haveColumn {
			continue
		}

		// add to source columns - we may use use any column for a transform
		sourceColumns = append(sourceColumns, NewSourceColumnDef(c))

		// only add the inferred column to 'Columns' if it satisfies MapFields
		// does this column match the any of the map fields?
		if r.ShouldMapSourceColumn(c.ColumnName) {
			r.Columns = append(r.Columns, c)
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
