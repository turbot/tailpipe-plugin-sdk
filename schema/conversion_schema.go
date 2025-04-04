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

// ConversionSchema is a specialised TableSchema which also contains a list of all source columns
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

	var sourceColumns []SourceColumnDef

	//get the table schema as a map
	schemaMap := r.AsMap()

	// First add all columns from the table schema
	for _, c := range tableSchema.Columns {
		sourceColumns = append(sourceColumns, NewSourceColumnDef(c))
	}

	// Then, if we are in autoMap mode, add any inferred columns that aren't already in the schema and are not excluded
	if len(r.MapFields) > 0 {
		for _, c := range inferredSchema.Columns {
			// if this column exists in the table def, skip it
			if _, haveColumn := schemaMap[c.ColumnName]; haveColumn {
				continue
			}

			// does this column match the any of the map fields?
			if !r.ShouldMapSourceColumn(c.ColumnName) {
				continue
			}

			// we do not have this column - add it
			r.Columns = append(r.Columns, c)
			// add to source columns
			sourceColumns = append(sourceColumns, NewSourceColumnDef(c))
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
