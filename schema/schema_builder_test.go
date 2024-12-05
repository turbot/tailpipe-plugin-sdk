package schema

import (
	"reflect"
	"testing"
	"time"
)

type SimpleStructNoTags struct {
	StringField      string
	IntegerField     int
	Int16Field       int16
	Int32Field       int32
	Int64Field       int64
	Float32Field     float32
	Float64Field     float64
	BooleanField     bool
	ByteField        byte
	Uint16Field      uint16
	Uint32Field      uint32
	Uint64Field      uint64
	ByteSliceField   []byte
	StringSliceField []string
}

type ComplexStructNoTags struct {
	TimeField              time.Time
	StructField            NestedStruct
	StringToStringMap      map[string]string
	StructSliceField       []NestedStruct
	StringToStructMap      map[string]NestedStruct
	StringToStructSliceMap map[string][]NestedStruct
}

type NestedStruct struct {
	StringField      string
	IntegerField     int
	InnerStructField InnerNestedStruct
}

type InnerNestedStruct struct {
	StringField string
}

type EmbeddedStruct struct {
	SimpleStructNoTags
	TopLevelStringField string
}

type InterfaceStruct struct {
	InterfaceField any
}

type CircStruct1 struct {
	F2 *CircStruct2
}
type CircStruct2 struct {
	F1 *CircStruct1
}

type RecursiveStruct1 struct {
	R2 *RecursiveStruct2
}
type RecursiveStruct2 struct {
	R3 *RecursiveStruct1
}
type RecursiveStruct3 struct {
	R4 *RecursiveStruct4
}

type RecursiveStruct4 struct {
	R5 *RecursiveStruct5
}
type RecursiveStruct5 struct {
	R6 *RecursiveStruct6
}
type RecursiveStruct6 struct {
	R6 *RecursiveStruct7
}
type RecursiveStruct7 struct {
	R6 *RecursiveStruct8
}
type RecursiveStruct8 struct {
	R6 *RecursiveStruct9
}
type RecursiveStruct9 struct {
	R6 *RecursiveStruct10
}
type RecursiveStruct10 struct {
	Str string
}

type SimpleStructWithTags struct {
	StringFieldNameOverridden        string `parquet:"name=renamed_string_field"`
	StringFieldNameJSONTag           string `json:"json_tag_only"`
	StringFieldNameJSONAndParquetTag string `parquet:"name=parquet_tag" json:"json_tag_and_parquet_tag"`
	IntegerFieldTypeOverridden       int    `parquet:"type=INTEGER"`
	Int16FieldBothOverridden         int16  `parquet:"name=renamed_int_16_field,type=INTEGER"`
}

type StructWithDeeplyNestedStructArray struct {
	StructWithNestedStructArray *StructWithStructArray
}

type StructWithStructArray struct {
	StructArrayField []*SimpleStructNoTags
}

// //
func TestSchemaFromStruct(t *testing.T) {
	type args struct {
		s any
	}

	// TODO #testing add parquet tag tests https://github.com/turbot/tailpipe-plugin-sdk/issues/20
	tests := []struct {
		name    string
		args    args
		want    *RowSchema
		wantErr bool
	}{
		{
			name: "simple no tags",
			args: args{
				s: SimpleStructNoTags{},
			},
			want: &RowSchema{
				Columns: []*ColumnSchema{
					{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
					{SourceName: "IntegerField", ColumnName: "integer_field", Type: "BIGINT"},
					{SourceName: "Int16Field", ColumnName: "int_16_field", Type: "SMALLINT"},
					{SourceName: "Int32Field", ColumnName: "int_32_field", Type: "INTEGER"},
					{SourceName: "Int64Field", ColumnName: "int_64_field", Type: "BIGINT"},
					{SourceName: "Float32Field", ColumnName: "float_32_field", Type: "FLOAT"},
					{SourceName: "Float64Field", ColumnName: "float_64_field", Type: "DOUBLE"},
					{SourceName: "BooleanField", ColumnName: "boolean_field", Type: "BOOLEAN"},
					{SourceName: "ByteField", ColumnName: "byte_field", Type: "UTINYINT"},
					{SourceName: "Uint16Field", ColumnName: "uint_16_field", Type: "USMALLINT"},
					{SourceName: "Uint32Field", ColumnName: "uint_32_field", Type: "UINTEGER"},
					{SourceName: "Uint64Field", ColumnName: "uint_64_field", Type: "UBIGINT"},
					{SourceName: "ByteSliceField", ColumnName: "byte_slice_field", Type: "BLOB"},
					{SourceName: "StringSliceField", ColumnName: "string_slice_field", Type: "VARCHAR[]"},
				},
			},
			wantErr: false,
		},
		{
			name: "complex no tags",
			args: args{
				s: ComplexStructNoTags{},
			},
			want: &RowSchema{
				Columns: []*ColumnSchema{
					{SourceName: "TimeField", ColumnName: "time_field", Type: "TIMESTAMP"},
					{
						SourceName: "StructField",
						ColumnName: "struct_field",
						Type:       "STRUCT",
						StructFields: []*ColumnSchema{
							{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
							{SourceName: "IntegerField", ColumnName: "integer_field", Type: "BIGINT"},
							{
								SourceName: "InnerStructField",
								ColumnName: "inner_struct_field",
								Type:       "STRUCT",
								StructFields: []*ColumnSchema{
									{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
								},
							},
						},
					},
					{
						SourceName: "StringToStringMap",
						ColumnName: "string_to_string_map",
						Type:       "JSON",
						StructFields: []*ColumnSchema{
							{Type: "VARCHAR"},
							{Type: "VARCHAR"},
						},
					},
					{
						SourceName: "StructSliceField",
						ColumnName: "struct_slice_field",
						Type:       "JSON",
					},

					{
						SourceName: "StringToStructMap",
						ColumnName: "string_to_struct_map",
						Type:       "JSON", StructFields: []*ColumnSchema{
							{Type: "VARCHAR"},
							{
								Type: "STRUCT",
								StructFields: []*ColumnSchema{
									{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
									{SourceName: "IntegerField", ColumnName: "integer_field", Type: "BIGINT"},
									{
										SourceName: "InnerStructField",
										ColumnName: "inner_struct_field",
										Type:       "STRUCT",
										StructFields: []*ColumnSchema{
											{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
										},
									},
								},
							},
						},
					},
					{
						SourceName: "StringToStructSliceMap",
						ColumnName: "string_to_struct_slice_map",
						Type:       "JSON",
						StructFields: []*ColumnSchema{
							{Type: "VARCHAR"},
							{
								Type: "STRUCT[]",
								StructFields: []*ColumnSchema{
									{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
									{SourceName: "IntegerField", ColumnName: "integer_field", Type: "BIGINT"},
									{
										SourceName: "InnerStructField",
										ColumnName: "inner_struct_field",
										Type:       "STRUCT",
										StructFields: []*ColumnSchema{
											{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
										},
									},
								},
							},
						}},
				},
			},

			wantErr: false,
		},
		{
			name: "embedded struct",
			args: args{
				s: EmbeddedStruct{},
			},
			want: &RowSchema{
				Columns: []*ColumnSchema{
					{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
					{SourceName: "IntegerField", ColumnName: "integer_field", Type: "BIGINT"},
					{SourceName: "Int16Field", ColumnName: "int_16_field", Type: "SMALLINT"},
					{SourceName: "Int32Field", ColumnName: "int_32_field", Type: "INTEGER"},
					{SourceName: "Int64Field", ColumnName: "int_64_field", Type: "BIGINT"},
					{SourceName: "Float32Field", ColumnName: "float_32_field", Type: "FLOAT"},
					{SourceName: "Float64Field", ColumnName: "float_64_field", Type: "DOUBLE"},
					{SourceName: "BooleanField", ColumnName: "boolean_field", Type: "BOOLEAN"},
					{SourceName: "ByteField", ColumnName: "byte_field", Type: "UTINYINT"},
					{SourceName: "Uint16Field", ColumnName: "uint_16_field", Type: "USMALLINT"},
					{SourceName: "Uint32Field", ColumnName: "uint_32_field", Type: "UINTEGER"},
					{SourceName: "Uint64Field", ColumnName: "uint_64_field", Type: "UBIGINT"},
					{SourceName: "ByteSliceField", ColumnName: "byte_slice_field", Type: "BLOB"},
					{SourceName: "StringSliceField", ColumnName: "string_slice_field", Type: "VARCHAR[]"},
					{SourceName: "TopLevelStringField", ColumnName: "top_level_string_field", Type: "VARCHAR"},
				},
			},

			wantErr: false,
		},
		{
			name: "interface struct",
			args: args{
				s: InterfaceStruct{},
			},

			wantErr: true,
		},
		{
			name: "circular struct",
			args: args{
				s: CircStruct1{},
			},

			wantErr: true,
		},
		{
			name: "recursive struct",
			args: args{
				s: RecursiveStruct1{},
			},

			wantErr: true,
		},
		{
			name: "simple with tags",
			args: args{
				s: SimpleStructWithTags{},
			},
			want: &RowSchema{
				Columns: []*ColumnSchema{
					{SourceName: "StringFieldNameOverridden", ColumnName: "renamed_string_field", Type: "VARCHAR"},
					{SourceName: "json_tag_only", ColumnName: "json_tag_only", Type: "VARCHAR"},
					{SourceName: "json_tag_and_parquet_tag", ColumnName: "parquet_tag", Type: "VARCHAR"},
					{SourceName: "IntegerFieldTypeOverridden", ColumnName: "integer_field_type_overridden", Type: "INTEGER"},
					{SourceName: "Int16FieldBothOverridden", ColumnName: "renamed_int_16_field", Type: "INTEGER"},
				},
			},
			wantErr: false,
		},
		{
			name: "struct array",
			args: args{
				s: StructWithStructArray{},
			},
			want: &RowSchema{
				// TODO we do not currently support struct arrays - treat as JSON https://github.com/turbot/tailpipe-plugin-sdk/issues/55
				Columns: []*ColumnSchema{
					{
						SourceName: "StructArrayField",
						ColumnName: "struct_array_field",
						Type:       "JSON",
					},
				},
				//want: &RowSchema{
				//Columns: []*ColumnSchema{
				//	{
				//		SourceName: "StructArrayField",
				//		ColumnName: "struct_array_field",
				//		Type:       "STRUCT[]",
				//		StructFields: []*ColumnSchema{
				//			{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
				//			{SourceName: "IntegerField", ColumnName: "integer_field", Type: "BIGINT"},
				//			{SourceName: "Int16Field", ColumnName: "int_16_field", Type: "SMALLINT"},
				//			{SourceName: "Int32Field", ColumnName: "int_32_field", Type: "INTEGER"},
				//			{SourceName: "Int64Field", ColumnName: "int_64_field", Type: "BIGINT"},
				//			{SourceName: "Float32Field", ColumnName: "float_32_field", Type: "FLOAT"},
				//			{SourceName: "Float64Field", ColumnName: "float_64_field", Type: "DOUBLE"},
				//			{SourceName: "BooleanField", ColumnName: "boolean_field", Type: "BOOLEAN"},
				//			{SourceName: "ByteField", ColumnName: "byte_field", Type: "UTINYINT"},
				//			{SourceName: "Uint16Field", ColumnName: "uint_16_field", Type: "USMALLINT"},
				//			{SourceName: "Uint32Field", ColumnName: "uint_32_field", Type: "UINTEGER"},
				//			{SourceName: "Uint64Field", ColumnName: "uint_64_field", Type: "UBIGINT"},
				//			{SourceName: "ByteSliceField", ColumnName: "byte_slice_field", Type: "BLOB"},
				//			{SourceName: "StringSliceField", ColumnName: "string_slice_field", Type: "VARCHAR[]"},
				//		},
				//	},
				//},
			},
			wantErr: false,
		},
		{
			// NOTE: SPECIAL CASE
			// parquet conversion does not (yet) handle deeply nested struct arrays so we treat as JSON
			name: "deeply nested struct array",
			args: args{
				s: StructWithDeeplyNestedStructArray{},
			},
			want: &RowSchema{
				Columns: []*ColumnSchema{
					{
						SourceName: "StructWithNestedStructArray",
						ColumnName: "struct_with_nested_struct_array",
						Type:       "STRUCT",
						StructFields: []*ColumnSchema{
							{SourceName: "StructArrayField",
								ColumnName: "struct_array_field",
								Type:       "JSON"},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SchemaFromStruct(tt.args.s)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("SchemaFromStruct() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if tt.wantErr {
				t.Errorf("SchemaFromStruct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i, c := range got.Columns {
				w := tt.want.Columns[i]
				if c.Type != w.Type {
					t.Errorf("Column %s Type = %v, want Type %v", c.ColumnName, c.Type, w.Type)
				}
				if c.ColumnName != w.ColumnName {
					t.Errorf("Column %d ColumnName = %v, want ColumnName %v", i, c.ColumnName, w.ColumnName)
				}
				if c.SourceName != w.SourceName {
					t.Errorf("Column %s SourceName = %v, want SourceName %v", c.ColumnName, c.SourceName, w.SourceName)
				}
				if c.Type == "ARRAY" || c.Type == "STRUCT" {
					if !reflect.DeepEqual(c.StructFields, w.StructFields) {
						t.Errorf("Column %s = %v, want StructFields %v", c.ColumnName, c.StructFields, w.StructFields)
					}
				}
			}

		})
	}
}
