package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormaliseColumnTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    *ColumnSchema
		expected string
	}{
		// Simple types
		{
			name: "simple type - varchar",
			input: &ColumnSchema{
				Type: "VARCHAR",
			},
			expected: "varchar",
		},
		{
			name: "simple type - integer",
			input: &ColumnSchema{
				Type: "INTEGER",
			},
			expected: "integer",
		},
		{
			name: "simple type - preserve non-duckdb type",
			input: &ColumnSchema{
				Type: "CustomType",
			},
			expected: "CustomType",
		},

		// Array types
		{
			name: "simple array",
			input: &ColumnSchema{
				Type: "VARCHAR[]",
			},
			expected: "varchar[]",
		},
		{
			name: "nested array",
			input: &ColumnSchema{
				Type: "VARCHAR[][]",
			},
			expected: "varchar[][]",
		},

		// Map types
		{
			name: "simple map",
			input: &ColumnSchema{
				Type: "MAP<VARCHAR, INTEGER>",
			},
			expected: "map<varchar, integer>",
		},
		{
			name: "map with array value",
			input: &ColumnSchema{
				Type: "MAP<VARCHAR, INTEGER[]>",
			},
			expected: "map<varchar, integer[]>",
		},
		{
			name: "map with array key",
			input: &ColumnSchema{
				Type: "MAP<VARCHAR[], INTEGER>",
			},
			expected: "map<varchar[], integer>",
		},

		// Struct types
		{
			name: "simple struct",
			input: &ColumnSchema{
				Type: "STRUCT(contentType VARCHAR, cacheControl VARCHAR)",
			},
			expected: "struct(contentType varchar, cacheControl varchar)",
		},
		{
			name: "struct with type names in field names",
			input: &ColumnSchema{
				Type: "STRUCT(varcharField VARCHAR, integerField INTEGER)",
			},
			expected: "struct(varcharField varchar, integerField integer)",
		},
		{
			name: "struct with array fields",
			input: &ColumnSchema{
				Type: "STRUCT(field1 VARCHAR[], field2 INTEGER[])",
			},
			expected: "struct(field1 varchar[], field2 integer[])",
		},

		// Union types
		{
			name: "simple union",
			input: &ColumnSchema{
				Type: "UNION(type1 VARCHAR, type2 INTEGER)",
			},
			expected: "union(type1 varchar, type2 integer)",
		},
		{
			name: "union with array types",
			input: &ColumnSchema{
				Type: "UNION(type1 VARCHAR[], type2 INTEGER[])",
			},
			expected: "union(type1 varchar[], type2 integer[])",
		},

		// Nested combinations
		{
			name: "struct with nested struct",
			input: &ColumnSchema{
				Type: "STRUCT(field1 STRUCT(nested1 VARCHAR, nested2 INTEGER), field2 VARCHAR)",
			},
			expected: "struct(field1 struct(nested1 varchar, nested2 integer), field2 varchar)",
		},
		{
			name: "struct with nested union",
			input: &ColumnSchema{
				Type: "STRUCT(field1 UNION(type1 VARCHAR, type2 INTEGER), field2 VARCHAR)",
			},
			expected: "struct(field1 union(type1 varchar, type2 integer), field2 varchar)",
		},
		{
			name: "union with nested struct",
			input: &ColumnSchema{
				Type: "UNION(type1 STRUCT(field1 VARCHAR), type2 INTEGER)",
			},
			expected: "union(type1 struct(field1 varchar), type2 integer)",
		},
		{
			name: "struct with nested map",
			input: &ColumnSchema{
				Type: "STRUCT(field1 MAP<VARCHAR, INTEGER>, field2 VARCHAR)",
			},
			expected: "struct(field1 map<varchar, integer>, field2 varchar)",
		},
		{
			name: "map with struct value",
			input: &ColumnSchema{
				Type: "MAP<VARCHAR, STRUCT(field1 VARCHAR)>",
			},
			expected: "map<varchar, struct(field1 varchar)>",
		},
		{
			name: "array of struct",
			input: &ColumnSchema{
				Type: "STRUCT(field1 VARCHAR, field2 INTEGER)[]",
			},
			expected: "struct(field1 varchar, field2 integer)[]",
		},
		{
			name: "array of union",
			input: &ColumnSchema{
				Type: "UNION(type1 VARCHAR, type2 INTEGER)[]",
			},
			expected: "union(type1 varchar, type2 integer)[]",
		},
		{
			name: "complex nested combination",
			input: &ColumnSchema{
				Type: "STRUCT(field1 MAP<STRUCT(key1 VARCHAR), UNION(type1 INTEGER, type2 VARCHAR[])>, field2 VARCHAR)[]",
			},
			expected: "struct(field1 map<struct(key1 varchar), union(type1 integer, type2 varchar[])>, field2 varchar)[]",
		},

		// Additional whitespace test cases
		{
			name: "struct with extra spaces",
			input: &ColumnSchema{
				Type: "STRUCT  (  field1   VARCHAR  ,   field2    INTEGER   )",
			},
			expected: "struct(field1 varchar, field2 integer)",
		},
		{
			name: "map with extra spaces",
			input: &ColumnSchema{
				Type: "MAP  <  VARCHAR  ,  INTEGER  >",
			},
			expected: "map<varchar, integer>",
		},
		{
			name: "nested struct with mixed spaces",
			input: &ColumnSchema{
				Type: "STRUCT(field1    STRUCT(  nested1   VARCHAR,nested2 INTEGER),   field2 VARCHAR)",
			},
			expected: "struct(field1 struct(nested1 varchar, nested2 integer), field2 varchar)",
		},
		{
			name: "union with inconsistent spaces",
			input: &ColumnSchema{
				Type: "UNION(  type1    VARCHAR[],type2    INTEGER[]  )",
			},
			expected: "union(type1 varchar[], type2 integer[])",
		},
		{
			name: "complex type with newlines and tabs",
			input: &ColumnSchema{
				Type: "STRUCT(\nfield1\tVARCHAR,\n\tfield2\tINTEGER\n)",
			},
			expected: "struct(field1 varchar, field2 integer)",
		},
		{
			name: "map with nested struct and extra spaces",
			input: &ColumnSchema{
				Type: "MAP  <  VARCHAR  ,  STRUCT  (  field1   VARCHAR  )  >",
			},
			expected: "map<varchar, struct(field1 varchar)>",
		},
		{
			name: "array with extra spaces",
			input: &ColumnSchema{
				Type: "VARCHAR  [  ]  [  ]",
			},
			expected: "varchar[][]",
		},
		{
			name: "struct with empty fields",
			input: &ColumnSchema{
				Type: "STRUCT(field1 VARCHAR,    ,field2 INTEGER)",
			},
			expected: "struct(field1 varchar, field2 integer)",
		},
		{
			name: "struct with multiple consecutive commas",
			input: &ColumnSchema{
				Type: "STRUCT(field1 VARCHAR,,,,field2 INTEGER)",
			},
			expected: "struct(field1 varchar, field2 integer)",
		},
		{
			name: "complex nested type with mixed spacing",
			input: &ColumnSchema{
				Type: "MAP  <  STRUCT  (  key   VARCHAR  )  ,  UNION  (  type1   INTEGER  ,  type2   VARCHAR  )  >  [  ]",
			},
			expected: "map<struct(key varchar), union(type1 integer, type2 varchar)>[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.input.NormaliseColumnTypes()
			if tt.input.Type != tt.expected {
				t.Errorf("NormaliseColumnTypes() = %v, want %v", tt.input.Type, tt.expected)
			}
		})
	}
}

func TestNormaliseComplexType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Simple types
		{
			name:     "simple type - varchar",
			input:    "VARCHAR",
			expected: "varchar",
		},
		{
			name:     "simple type - preserve non-duckdb type",
			input:    "CustomType",
			expected: "CustomType",
		},

		// Array types
		{
			name:     "simple array",
			input:    "VARCHAR[]",
			expected: "varchar[]",
		},
		{
			name:     "nested array",
			input:    "VARCHAR[][]",
			expected: "varchar[][]",
		},

		// Map types
		{
			name:     "simple map",
			input:    "MAP<VARCHAR, INTEGER>",
			expected: "map<varchar, integer>",
		},
		{
			name:     "map with array value",
			input:    "MAP<VARCHAR, INTEGER[]>",
			expected: "map<varchar, integer[]>",
		},

		// Struct types
		{
			name:     "simple struct",
			input:    "STRUCT(contentType VARCHAR, cacheControl VARCHAR)",
			expected: "struct(contentType varchar, cacheControl varchar)",
		},
		{
			name:     "empty struct",
			input:    "STRUCT()",
			expected: "struct()",
		},
		{
			name:     "single field struct",
			input:    "STRUCT(field1 VARCHAR)",
			expected: "struct(field1 varchar)",
		},

		// Union types
		{
			name:     "simple union",
			input:    "UNION(type1 VARCHAR, type2 INTEGER)",
			expected: "union(type1 varchar, type2 integer)",
		},
		{
			name:     "empty union",
			input:    "UNION()",
			expected: "union()",
		},

		// Complex combinations
		{
			name:     "struct with nested map",
			input:    "STRUCT(field1 MAP<VARCHAR, INTEGER>, field2 VARCHAR)",
			expected: "struct(field1 map<varchar, integer>, field2 varchar)",
		},
		{
			name:     "map with struct value",
			input:    "MAP<VARCHAR, STRUCT(field1 VARCHAR)>",
			expected: "map<varchar, struct(field1 varchar)>",
		},
		{
			name:     "array of struct",
			input:    "STRUCT(field1 VARCHAR)[]",
			expected: "struct(field1 varchar)[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := &ColumnSchema{}
			result := cs.normaliseType(tt.input)
			if result != tt.expected {
				t.Errorf("normaliseType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestColumnSchema_Clone(t *testing.T) {
	type fields struct {
		SourceName   string
		ColumnName   string
		Type         string
		StructFields []*ColumnSchema
		Description  string
		Required     bool
		NullIf       string
		Transform    string
	}
	tests := []struct {
		name   string
		fields fields
		want   *ColumnSchema
	}{
		{
			name: "basic fields",
			fields: fields{
				SourceName:  "source1",
				ColumnName:  "col1",
				Type:        "varchar",
				Description: "test description",
				Required:    true,
				NullIf:      "null",
				Transform:   "upper",
			},
			want: &ColumnSchema{
				SourceName:  "source1",
				ColumnName:  "col1",
				Type:        "varchar",
				Description: "test description",
				Required:    true,
				NullIf:      "null",
				Transform:   "upper",
			},
		},
		{
			name: "with struct fields",
			fields: fields{
				SourceName: "source2",
				ColumnName: "col2",
				Type:       "struct",
				StructFields: []*ColumnSchema{
					{
						ColumnName: "nested1",
						Type:       "varchar",
					},
					{
						ColumnName: "nested2",
						Type:       "integer",
					},
				},
			},
			want: &ColumnSchema{
				SourceName: "source2",
				ColumnName: "col2",
				Type:       "struct",
				StructFields: []*ColumnSchema{
					{
						ColumnName: "nested1",
						Type:       "varchar",
					},
					{
						ColumnName: "nested2",
						Type:       "integer",
					},
				},
			},
		},
		{
			name:   "empty fields",
			fields: fields{},
			want:   &ColumnSchema{},
		},
		{
			name: "complex nested structure",
			fields: fields{
				SourceName: "source3",
				ColumnName: "col3",
				Type:       "struct",
				StructFields: []*ColumnSchema{
					{
						ColumnName: "nested1",
						Type:       "struct",
						StructFields: []*ColumnSchema{
							{
								ColumnName: "deep1",
								Type:       "varchar",
							},
						},
					},
				},
				Description: "complex nested structure",
				Required:    true,
			},
			want: &ColumnSchema{
				SourceName: "source3",
				ColumnName: "col3",
				Type:       "struct",
				StructFields: []*ColumnSchema{
					{
						ColumnName: "nested1",
						Type:       "struct",
						StructFields: []*ColumnSchema{
							{
								ColumnName: "deep1",
								Type:       "varchar",
							},
						},
					},
				},
				Description: "complex nested structure",
				Required:    true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ColumnSchema{
				SourceName:   tt.fields.SourceName,
				ColumnName:   tt.fields.ColumnName,
				Type:         tt.fields.Type,
				StructFields: tt.fields.StructFields,
				Description:  tt.fields.Description,
				Required:     tt.fields.Required,
				NullIf:       tt.fields.NullIf,
				Transform:    tt.fields.Transform,
			}
			got := c.Clone()

			// Test that the clone has the same values
			assert.Equal(t, tt.want.SourceName, got.SourceName)
			assert.Equal(t, tt.want.ColumnName, got.ColumnName)
			assert.Equal(t, tt.want.Type, got.Type)
			assert.Equal(t, tt.want.Description, got.Description)
			assert.Equal(t, tt.want.Required, got.Required)
			assert.Equal(t, tt.want.NullIf, got.NullIf)
			assert.Equal(t, tt.want.Transform, got.Transform)

			// Test that the clone is a deep copy
			if c.StructFields != nil {
				// Verify the slice is a new instance
				assert.NotSame(t, &c.StructFields, &got.StructFields, "StructFields should be a new slice")

				// Verify each nested field is a new instance
				for i := range c.StructFields {
					assert.NotSame(t, c.StructFields[i], got.StructFields[i], "Nested StructFields should be new instances")

					// For complex nested structures, verify deeper nesting
					if c.StructFields[i].StructFields != nil {
						assert.NotSame(t, &c.StructFields[i].StructFields, &got.StructFields[i].StructFields,
							"Deeply nested StructFields should be new instances")
					}
				}
			}
		})
	}
}
