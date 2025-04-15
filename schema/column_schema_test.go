package schema

import (
	"testing"
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
