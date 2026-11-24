package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func columnNamesOf(s *ConversionSchema) []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.ColumnName
	}
	return names
}

// The conversion schema orders its columns alphabetically, tp_ columns last, and ColumnString lists exactly those columns.
func Test_NewConversionSchema_ColumnOrder(t *testing.T) {
	tableSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"},
			{ColumnName: "zeta", Type: "varchar", SourceName: "zeta"},
			{ColumnName: "tp_index", Type: "varchar", SourceName: "tp_index"},
			{ColumnName: "alpha", Type: "varchar", SourceName: "alpha"},
		},
	}

	result := NewConversionSchema(tableSchema)

	assert.Equal(t, []string{"alpha", "zeta", "tp_index", "tp_timestamp"}, columnNamesOf(result))
	assert.Equal(t, `"alpha", "zeta", "tp_index", "tp_timestamp"`, result.ColumnString)
}

// A column with a transform has no source column, and is still a column of the output.
func Test_NewConversionSchema_TransformColumn(t *testing.T) {
	tableSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"},
			{ColumnName: "user_name", Type: "varchar", Transform: "lower(raw_user)"},
			{ColumnName: "event", Type: "varchar", SourceName: "event"},
		},
	}

	var result *ConversionSchema
	require.NotPanics(t, func() { result = NewConversionSchema(tableSchema) })

	assert.Equal(t, []string{"event", "user_name", "tp_timestamp"}, columnNamesOf(result))
	assert.Equal(t, `"event", "user_name", "tp_timestamp"`, result.ColumnString)
	assert.Len(t, result.SourceColumns, 2)
}

// An inferred source column that the table does not map is read as a source column but is not an output column,
// so it is not in ColumnString.
func Test_NewConversionSchemaWithInferredSchema_UnmappedSourceColumn(t *testing.T) {
	tableSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"},
			{ColumnName: "event", Type: "varchar", SourceName: "event"},
		},
		MapFields: []string{"event"},
	}
	inferredSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "raw_extra", Type: "varchar", SourceName: "raw_extra"},
		},
	}

	result := NewConversionSchemaWithInferredSchema(tableSchema, inferredSchema)

	assert.Equal(t, []string{"event", "tp_timestamp"}, columnNamesOf(result))
	assert.Equal(t, `"event", "tp_timestamp"`, result.ColumnString)
	assert.Len(t, result.SourceColumns, 3)
}

// A transform column and mapped inferred columns together.
func Test_NewConversionSchemaWithInferredSchema_TransformAndInferred(t *testing.T) {
	tableSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"},
			{ColumnName: "user_name", Type: "varchar", Transform: "lower(raw_user)"},
			{ColumnName: "zone", Type: "varchar", Transform: "'utc'"},
		},
		MapFields: []string{"*"},
	}
	inferredSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "raw_user", Type: "varchar", SourceName: "raw_user"},
		},
	}

	var result *ConversionSchema
	require.NotPanics(t, func() { result = NewConversionSchemaWithInferredSchema(tableSchema, inferredSchema) })

	assert.Equal(t, []string{"raw_user", "user_name", "zone", "tp_timestamp"}, columnNamesOf(result))
	assert.Equal(t, `"raw_user", "user_name", "zone", "tp_timestamp"`, result.ColumnString)
}

// Building a conversion schema leaves the table schema it was given as it was.
func Test_NewConversionSchemaWithInferredSchema_LeavesTableSchema(t *testing.T) {
	// a backing array with spare room: an append into the table schema's columns would write into it
	backing := make([]*ColumnSchema, 4)
	backing[0] = &ColumnSchema{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"}
	backing[1] = &ColumnSchema{ColumnName: "event", Type: "varchar", SourceName: "event"}
	tableSchema := &TableSchema{Columns: backing[:2], MapFields: []string{"*"}}
	inferredSchema := &TableSchema{
		Columns: []*ColumnSchema{{ColumnName: "extra", Type: "varchar", SourceName: "extra"}},
	}

	NewConversionSchemaWithInferredSchema(tableSchema, inferredSchema)

	require.Len(t, tableSchema.Columns, 2)
	assert.Equal(t, "tp_timestamp", tableSchema.Columns[0].ColumnName)
	assert.Equal(t, "event", tableSchema.Columns[1].ColumnName)
	assert.Nil(t, backing[2], "the table schema's spare capacity was written to")
}

// A column name that holds a double quote is escaped in ColumnString, so it stays one identifier there.
func Test_NewConversionSchemaWithInferredSchema_QuotesColumnNames(t *testing.T) {
	tableSchema := &TableSchema{
		Columns:   []*ColumnSchema{{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"}},
		MapFields: []string{"*"},
	}
	inferredSchema := &TableSchema{
		Columns: []*ColumnSchema{{ColumnName: `a", "b`, Type: "varchar", SourceName: `a", "b`}},
	}

	result := NewConversionSchemaWithInferredSchema(tableSchema, inferredSchema)

	assert.Equal(t, `"a"", ""b", "tp_timestamp"`, result.ColumnString)
}

// A declared column keeps its transform when the source data has a field of the same name, which the transform reads.
func Test_NewConversionSchemaWithInferredSchema_TransformSharesSourceName(t *testing.T) {
	tableSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"},
			{ColumnName: "level", Type: "varchar", Transform: "upper(level)"},
		},
		MapFields: []string{"*"},
	}
	inferredSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "level", Type: "varchar", SourceName: "level"},
			{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"},
		},
	}

	result := NewConversionSchemaWithInferredSchema(tableSchema, inferredSchema)

	assert.Equal(t, []string{"level", "tp_timestamp"}, columnNamesOf(result))
	assert.Equal(t, "upper(level)", result.Columns[0].Transform)
	assert.Equal(t, `"level", "tp_timestamp"`, result.ColumnString)
	// the transform's input is still read from the source data
	assert.Contains(t, result.SourceColumns, SourceColumnDef{Name: "level", Type: "varchar"})
}

// A declared column keeps its source and type when the source data also has a field named like the column.
func Test_NewConversionSchemaWithInferredSchema_RenamedColumnSharesName(t *testing.T) {
	tableSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"},
			{ColumnName: "user", Type: "varchar", SourceName: "raw_user"},
		},
		MapFields: []string{"*"},
	}
	inferredSchema := &TableSchema{
		Columns: []*ColumnSchema{
			{ColumnName: "raw_user", Type: "varchar", SourceName: "raw_user"},
			{ColumnName: "user", Type: "bigint", SourceName: "user"},
			{ColumnName: "tp_timestamp", Type: "timestamp", SourceName: "tp_timestamp"},
		},
	}

	result := NewConversionSchemaWithInferredSchema(tableSchema, inferredSchema)

	// raw_user feeds the declared column, so it is not also mapped as a column of its own
	assert.Equal(t, []string{"user", "tp_timestamp"}, columnNamesOf(result))
	user := result.Columns[0]
	assert.Equal(t, "raw_user", user.SourceName)
	assert.Equal(t, "varchar", user.Type)
}
