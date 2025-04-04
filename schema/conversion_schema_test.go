package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewConversionSchemaWithInferredSchema(t *testing.T) {
	testCases := []struct {
		name           string
		tableSchema    *TableSchema
		inferredSchema *TableSchema
		expectedCheck  func(t *testing.T, result *ConversionSchema)
	}{
		{
			name: "autoMap true, no excludes",
			tableSchema: &TableSchema{
				Columns: []*ColumnSchema{
					{
						ColumnName: "existing_col",
						Type:       "string",
						SourceName: "existing_col",
					},
				},
				Select: "*",
			},
			inferredSchema: &TableSchema{
				Columns: []*ColumnSchema{
					{
						ColumnName: "inferred_col",
						Type:       "integer",
						SourceName: "inferred_col",
					},
				},
			},
			expectedCheck: func(t *testing.T, result *ConversionSchema) {
				assert.Equal(t, 2, len(result.Columns))
				assert.Equal(t, 2, len(result.SourceColumns))

				// Check existing column
				existingCol := result.AsMap()["existing_col"]
				assert.NotNil(t, existingCol)
				assert.Equal(t, "string", existingCol.Type)

				// Check inferred column
				inferredCol := result.AsMap()["inferred_col"]
				assert.NotNil(t, inferredCol)
				assert.Equal(t, "integer", inferredCol.Type)
			},
		},
		{
			name: "autoMap false, no excludes",
			tableSchema: &TableSchema{
				Columns: []*ColumnSchema{
					{
						ColumnName: "existing_col",
						Type:       "string",
						SourceName: "existing_col",
					},
				},
				Select: "",
			},
			inferredSchema: &TableSchema{
				Columns: []*ColumnSchema{
					{
						ColumnName: "inferred_col",
						Type:       "integer",
						SourceName: "inferred_col",
					},
				},
			},
			expectedCheck: func(t *testing.T, result *ConversionSchema) {
				assert.Equal(t, 1, len(result.Columns))
				assert.Equal(t, 1, len(result.SourceColumns))

				// Check existing column
				existingCol := result.AsMap()["existing_col"]
				assert.NotNil(t, existingCol)
				assert.Equal(t, "string", existingCol.Type)

				// Verify inferred column is not present
				inferredCol := result.AsMap()["inferred_col"]
				assert.Nil(t, inferredCol)
			},
		},
		//{
		//	name: "autoMap true, with excludes",
		//	tableSchema: &TableSchema{
		//		Columns: []*ColumnSchema{
		//			{
		//				ColumnName: "existing_col",
		//				Type:       "string",
		//				SourceName: "existing_col",
		//			},
		//		},
		//		Select:              "*",
		//		ExcludeSourceFields: []string{"excluded_col"},
		//	},
		//	inferredSchema: &TableSchema{
		//		Columns: []*ColumnSchema{
		//			{
		//				ColumnName: "excluded_col",
		//				Type:       "integer",
		//				SourceName: "excluded_col",
		//			},
		//			{
		//				ColumnName: "inferred_col",
		//				Type:       "boolean",
		//				SourceName: "inferred_col",
		//			},
		//		},
		//	},
		//	expectedCheck: func(t *testing.T, result *ConversionSchema) {
		//		assert.Equal(t, 2, len(result.Columns))
		//		assert.Equal(t, 2, len(result.SourceColumns))
		//
		//		// Check existing column
		//		existingCol := result.AsMap()["existing_col"]
		//		assert.NotNil(t, existingCol)
		//		assert.Equal(t, "string", existingCol.Type)
		//
		//		// Verify excluded column is not present
		//		excludedCol := result.AsMap()["excluded_col"]
		//		assert.Nil(t, excludedCol)
		//
		//		// Check inferred column
		//		inferredCol := result.AsMap()["inferred_col"]
		//		assert.NotNil(t, inferredCol)
		//		assert.Equal(t, "boolean", inferredCol.Type)
		//	},
		//},
		//{
		//	name: "autoMap false, with excludes",
		//	tableSchema: &TableSchema{
		//		Columns: []*ColumnSchema{
		//			{
		//				ColumnName: "existing_col",
		//				Type:       "string",
		//				SourceName: "existing_col",
		//			},
		//		},
		//		Select:              "",
		//		ExcludeSourceFields: []string{"excluded_col"},
		//	},
		//	inferredSchema: &TableSchema{
		//		Columns: []*ColumnSchema{
		//			{
		//				ColumnName: "excluded_col",
		//				Type:       "integer",
		//				SourceName: "excluded_col",
		//			},
		//			{
		//				ColumnName: "inferred_col",
		//				Type:       "boolean",
		//				SourceName: "inferred_col",
		//			},
		//		},
		//	},
		//	expectedCheck: func(t *testing.T, result *ConversionSchema) {
		//		assert.Equal(t, 1, len(result.Columns))
		//		assert.Equal(t, 1, len(result.SourceColumns))
		//
		//		// Check existing column
		//		existingCol := result.AsMap()["existing_col"]
		//		assert.NotNil(t, existingCol)
		//		assert.Equal(t, "string", existingCol.Type)
		//
		//		// Verify neither excluded nor inferred columns are present
		//		excludedCol := result.AsMap()["excluded_col"]
		//		assert.Nil(t, excludedCol)
		//		inferredCol := result.AsMap()["inferred_col"]
		//		assert.Nil(t, inferredCol)
		//	},
		//},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewConversionSchemaWithInferredSchema(tc.tableSchema, tc.inferredSchema)
			tc.expectedCheck(t, result)
		})
	}
}

func Test_NewConversionSchema(t *testing.T) {
	testCases := []struct {
		name          string
		tableSchema   *TableSchema
		expectedCheck func(t *testing.T, result *ConversionSchema)
	}{
		{
			name: "basic schema",
			tableSchema: &TableSchema{
				Columns: []*ColumnSchema{
					{
						ColumnName: "col1",
						Type:       "string",
						SourceName: "col1",
					},
					{
						ColumnName: "col2",
						Type:       "integer",
						SourceName: "col2",
					},
				},
			},
			expectedCheck: func(t *testing.T, result *ConversionSchema) {
				assert.Equal(t, 2, len(result.Columns))
				assert.Equal(t, 2, len(result.SourceColumns))

				// Check columns
				col1 := result.AsMap()["col1"]
				assert.NotNil(t, col1)
				assert.Equal(t, "string", col1.Type)

				col2 := result.AsMap()["col2"]
				assert.NotNil(t, col2)
				assert.Equal(t, "integer", col2.Type)
			},
		},
		{
			name: "empty schema",
			tableSchema: &TableSchema{
				Columns: []*ColumnSchema{},
			},
			expectedCheck: func(t *testing.T, result *ConversionSchema) {
				assert.Equal(t, 0, len(result.Columns))
				assert.Equal(t, 0, len(result.SourceColumns))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewConversionSchema(tc.tableSchema)
			tc.expectedCheck(t, result)
		})
	}
}
