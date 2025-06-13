package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MergeWithCommonSchema(t *testing.T) {
	testCases := []struct {
		name          string
		inputColumns  []*ColumnSchema
		expectedCheck func(t *testing.T, merged *TableSchema)
	}{
		{
			name:         "empty schema gets all common fields",
			inputColumns: []*ColumnSchema{},
			expectedCheck: func(t *testing.T, merged *TableSchema) {
				commonSchema := CommonFieldsSchema()
				assert.Equal(t, len(commonSchema.Columns), len(merged.Columns))

				mergedMap := merged.AsMap()
				for _, commonCol := range commonSchema.Columns {
					col := mergedMap[commonCol.ColumnName]
					assert.NotNil(t, col)
					assert.Equal(t, commonCol.Type, col.Type)
					assert.Equal(t, commonCol.Required, col.Required)
					assert.Equal(t, commonCol.Description, col.Description)
					assert.Equal(t, commonCol.SourceName, col.SourceName)
				}
			},
		},
		{
			name: "common field with all custom properties",
			inputColumns: []*ColumnSchema{
				{
					ColumnName:  "tp_id",
					SourceName:  "custom_source",
					Type:        "integer", // should be overridden
					Required:    false,     // should be overridden
					Description: "custom description",
				},
			},
			expectedCheck: func(t *testing.T, merged *TableSchema) {
				col := merged.AsMap()["tp_id"]
				assert.NotNil(t, col)
				assert.Equal(t, "custom_source", col.SourceName, "should keep custom source")
				assert.Equal(t, "varchar", col.Type, "should use common type")
				assert.True(t, col.Required, "should use common required")
				assert.Equal(t, "custom description", col.Description, "should keep custom description")
			},
		},
		{
			name: "common field with only source name",
			inputColumns: []*ColumnSchema{
				{
					ColumnName: "tp_id",
					SourceName: "custom_source",
				},
			},
			expectedCheck: func(t *testing.T, merged *TableSchema) {
				col := merged.AsMap()["tp_id"]
				assert.NotNil(t, col)
				assert.Equal(t, "custom_source", col.SourceName, "should keep custom source")
				assert.Equal(t, "varchar", col.Type, "should use common type")
				assert.True(t, col.Required, "should use common required")
				assert.NotEmpty(t, col.Description, "should use common description")
			},
		},
		{
			name: "common field with empty description",
			inputColumns: []*ColumnSchema{
				{
					ColumnName:  "tp_id",
					SourceName:  "custom_source",
					Description: "",
				},
			},
			expectedCheck: func(t *testing.T, merged *TableSchema) {
				col := merged.AsMap()["tp_id"]
				assert.NotNil(t, col)
				commonCol := CommonFieldsSchema().AsMap()["tp_id"]
				assert.Equal(t, commonCol.Description, col.Description, "should use common description when empty")
			},
		},
		{
			name: "mix of common and custom fields",
			inputColumns: []*ColumnSchema{
				{
					ColumnName: "tp_id",
					SourceName: "custom_source",
				},
				{
					ColumnName:  "custom_field",
					SourceName:  "custom_source",
					Type:        "varchar",
					Required:    true,
					Description: "custom description",
				},
			},
			expectedCheck: func(t *testing.T, merged *TableSchema) {
				// Check common field
				commonCol := merged.AsMap()["tp_id"]
				assert.NotNil(t, commonCol)
				assert.Equal(t, "custom_source", commonCol.SourceName)
				assert.Equal(t, "varchar", commonCol.Type)
				assert.True(t, commonCol.Required)

				// Check custom field
				customCol := merged.AsMap()["custom_field"]
				assert.NotNil(t, customCol)
				assert.Equal(t, "custom_source", customCol.SourceName)
				assert.Equal(t, "varchar", customCol.Type)
				assert.True(t, customCol.Required)
				assert.Equal(t, "custom description", customCol.Description)

				// Check all common fields are present
				commonSchema := CommonFieldsSchema()
				for _, col := range commonSchema.Columns {
					assert.NotNil(t, merged.AsMap()[col.ColumnName])
				}
			},
		},
		{
			name: "common field with blank source name",
			inputColumns: []*ColumnSchema{
				{
					ColumnName: "tp_id",
					SourceName: "",
				},
			},
			expectedCheck: func(t *testing.T, merged *TableSchema) {
				col := merged.AsMap()["tp_id"]
				assert.NotNil(t, col)
				commonCol := CommonFieldsSchema().AsMap()["tp_id"]
				assert.Equal(t, commonCol.SourceName, col.SourceName, "should use common source name when empty")
			},
		},
		{
			name: "multiple common fields with different overrides",
			inputColumns: []*ColumnSchema{
				{
					ColumnName:  "tp_id",
					SourceName:  "custom_source_1",
					Description: "custom desc 1",
				},
				{
					ColumnName: "tp_timestamp",
					SourceName: "custom_source_2",
				},
				{
					ColumnName:  "tp_source_type",
					Description: "custom desc 3",
				},
			},
			expectedCheck: func(t *testing.T, merged *TableSchema) {
				// Check tp_id
				idCol := merged.AsMap()["tp_id"]
				assert.NotNil(t, idCol)
				assert.Equal(t, "custom_source_1", idCol.SourceName)
				assert.Equal(t, "custom desc 1", idCol.Description)

				// Check tp_timestamp
				timeCol := merged.AsMap()["tp_timestamp"]
				assert.NotNil(t, timeCol)
				assert.Equal(t, "custom_source_2", timeCol.SourceName)
				assert.Equal(t, CommonFieldsSchema().AsMap()["tp_timestamp"].Description, timeCol.Description)

				// Check tp_source_type
				sourceCol := merged.AsMap()["tp_source_type"]
				assert.NotNil(t, sourceCol)
				assert.Equal(t, CommonFieldsSchema().AsMap()["tp_source_type"].SourceName, sourceCol.SourceName)
				assert.Equal(t, "custom desc 3", sourceCol.Description)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema := &TableSchema{
				Name:    "test_table",
				Columns: tc.inputColumns,
			}
			merged := schema.MergeWithCommonSchema()
			tc.expectedCheck(t, merged)
		})
	}
}

func TestTableSchema_Validate(t *testing.T) {
	type fields struct {
		Name        string
		Columns     []*ColumnSchema
		MapFields   []string
		Description string
		NullIf      string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "valid schema with required columns",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "id",
						Type:       "integer",
						Required:   true,
					},
					{
						ColumnName: "name",
						Type:       "varchar",
						Required:   true,
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "valid schema with optional columns",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "id",
						Type:       "integer",
						Required:   true,
					},
					{
						ColumnName: "description",
						Type:       "varchar",
						Required:   false,
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid schema with optional column missing type",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "id",
						Type:       "integer",
						Required:   true,
					},
					{
						ColumnName: "description",
						Required:   false,
					},
				},
			},
			wantErr: func(t assert.TestingT, err error, msgAndArgs ...interface{}) bool {
				return assert.ErrorContains(t, err, "column type must be specified if column is optional")
			},
		},
		{
			name: "invalid schema with invalid column type",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "id",
						Type:       "invalid_type",
						Required:   true,
					},
				},
			},
			wantErr: func(t assert.TestingT, err error, msgAndArgs ...interface{}) bool {
				return assert.ErrorContains(t, err, "invalid column type")
			},
		},
		{
			name: "invalid schema with struct array type",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "data",
						Type:       "struct[]",
						Required:   true,
					},
				},
			},
			wantErr: func(t assert.TestingT, err error, msgAndArgs ...interface{}) bool {
				return assert.ErrorContains(t, err, "invalid column type")
			},
		},
		{
			name: "valid schema with array type",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "tags",
						Type:       "varchar[]",
						Required:   true,
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "valid schema with struct type",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "metadata",
						Type:       "struct",
						Required:   true,
						StructFields: []*ColumnSchema{
							{
								ColumnName: "created_at",
								Type:       "timestamp",
								Required:   true,
							},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid schema with invalid struct field type",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "metadata",
						Type:       "struct",
						Required:   true,
						StructFields: []*ColumnSchema{
							{
								ColumnName: "created_at",
								Type:       "invalid_type",
								Required:   true,
							},
						},
					},
				},
			},
			wantErr: func(t assert.TestingT, err error, msgAndArgs ...interface{}) bool {
				return assert.ErrorContains(t, err, "invalid column type")
			},
		},
		{
			name: "valid schema with nested struct fields",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "user",
						Type:       "struct",
						Required:   true,
						StructFields: []*ColumnSchema{
							{
								ColumnName: "name",
								Type:       "varchar",
								Required:   true,
							},
							{
								ColumnName: "address",
								Type:       "struct",
								Required:   true,
								StructFields: []*ColumnSchema{
									{
										ColumnName: "street",
										Type:       "varchar",
										Required:   true,
									},
									{
										ColumnName: "city",
										Type:       "varchar",
										Required:   true,
									},
								},
							},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid schema with optional struct field missing type",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "metadata",
						Type:       "struct",
						Required:   true,
						StructFields: []*ColumnSchema{
							{
								ColumnName: "created_at",
								Required:   false,
							},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "valid schema with struct field source name",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "metadata",
						Type:       "struct",
						Required:   true,
						StructFields: []*ColumnSchema{
							{
								ColumnName: "created_at",
								SourceName: "createdAt",
								Type:       "timestamp",
								Required:   true,
							},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "valid schema with struct field transform",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "metadata",
						Type:       "struct",
						Required:   true,
						StructFields: []*ColumnSchema{
							{
								ColumnName: "created_at",
								Transform:  "to_timestamp(created_at)",
								Type:       "timestamp",
								Required:   true,
							},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "valid schema with struct field array type",
			fields: fields{
				Name: "test_table",
				Columns: []*ColumnSchema{
					{
						ColumnName: "metadata",
						Type:       "struct",
						Required:   true,
						StructFields: []*ColumnSchema{
							{
								ColumnName: "tags",
								Type:       "varchar[]",
								Required:   true,
							},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &TableSchema{
				Name:        tt.fields.Name,
				Columns:     tt.fields.Columns,
				MapFields:   tt.fields.MapFields,
				Description: tt.fields.Description,
				NullIf:      tt.fields.NullIf,
			}
			tt.wantErr(t, r.Validate(), "Validate()")
		})
	}
}
