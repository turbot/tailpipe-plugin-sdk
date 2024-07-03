package schema

import (
	"errors"
	"fmt"
	"github.com/iancoleman/strcase"
	"reflect"
	"time"
)

func SchemaFromStruct(s any) (*RowSchema, error) {
	// Get the type of the rowStruct
	t := reflect.TypeOf(s)
	return SchemaFromType(t)
}

func SchemaFromType(t reflect.Type) (*RowSchema, error) {
	// reflect over parquet tags to build schema
	var res = &RowSchema{}

	// If rowStruct is a pointer, get the element type
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var errorList []error
	// Iterate over the struct fields
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Get the parquet tag
		var p = &ParquetTag{}
		var err error

		if tag := field.Tag.Get("parquet"); tag != "" {

			p, err = ParseParquetTag(tag)
			if err != nil {
				errorList = append(errorList, err)
				continue
			}
			// is this field skipped?
			if p.Skip {
				continue
			}
		}

		// if the tag does not specify a name, use the field name
		if p.Name == "" {
			p.Name = strcase.ToSnake(field.Name)
		}

		var c *ColumnSchema
		// if the tag does not specify a type, infer from the field type
		if p.Type != "" {
			// type explicitly set in the tag (struct not supported)
			c = &ColumnSchema{
				SourceName: field.Name,
				ColumnName: p.Name,
				Type:       p.Type,
			}
		} else {
			columnType, err := getColumnSchemaType(field.Type)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("failed to get schema for field %s: %w", field.Name, err))
				continue
			}
			c = &ColumnSchema{
				SourceName:  field.Name,
				ColumnName:  p.Name,
				Type:        columnType.Type,
				ChildFields: columnType.ChildFields,
			}
		}

		res.Columns = append(res.Columns, c)
	}

	if len(errorList) > 0 {
		return nil, errors.Join(errorList...)
	}

	return res, nil

}

func getColumnSchemaType(t reflect.Type) (ColumnType, error) {
	c := ColumnType{}

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Bool:
		c.Type = "BOOLEAN"
	case reflect.Int8:
		c.Type = "TINYINT"
	case reflect.Int16:
		c.Type = "SMALLINT"
	case reflect.Int32:
		c.Type = "INTEGER"
	case reflect.Int, reflect.Int64:
		c.Type = "BIGINT"
	case reflect.Uint8:
		c.Type = "UTINYINT"
	case reflect.Uint16:
		c.Type = "USMALLINT"
	case reflect.Uint32:
		c.Type = "UINTEGER"
	case reflect.Uint, reflect.Uint64:
		c.Type = "UBIGINT"
	case reflect.Float32:
		c.Type = "FLOAT"
	case reflect.Float64:
		c.Type = "DOUBLE"
	case reflect.String:
		c.Type = "VARCHAR"
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			c.Type = "BLOB"
			break
		}
		listType, err := getColumnSchemaType(t.Elem())
		if err != nil {
			c.Type = ""
		}
		c.Type = fmt.Sprintf("%s[]", listType.Type)
		// for struct types, we need to wrap the child fields in a new ColumnSchema
		if listType.Type == "STRUCT" {
			c.ChildFields = []*ColumnSchema{{Type: listType.Type, ChildFields: listType.ChildFields}}
		}
	case reflect.Struct:
		// check if this is a time.Time
		if t == reflect.TypeOf(time.Time{}) {
			c.Type = "TIMESTAMP"
			break
		}
		// get the struct schema and convert into a DuckDB struct
		schema, err := SchemaFromType(t)
		if err != nil {
			return c, err
		}
		// convert the schema into a DuckDB struct definition
		c.Type = "STRUCT"
		c.ChildFields = schema.Columns
	case reflect.Map:
		c.Type = "MAP"
		// get the key and value types
		keyType, err := getColumnSchemaType(t.Key())
		if err != nil {
			return c, err
		}
		valueType, err := getColumnSchemaType(t.Elem())
		if err != nil {
			return c, err
		}
		c.ChildFields = []*ColumnSchema{{Type: keyType.Type, ChildFields: keyType.ChildFields}, {Type: valueType.Type, ChildFields: valueType.ChildFields}}
	default:

		return c, fmt.Errorf("unsupported type %s", t)
	}
	return c, nil
}

//func schemaToDuckDBStruct(schema *RowSchema) (string, error) {
//	/*
//		 write a duck db struct def as follows:
//
//			STRUCT(
//				field1 as column1_name VARCHAR,
//				field2 as column2_name INTEGER
//			)
//	*/
//
//	var str strings.Builder
//	str.WriteString("STRUCT(")
//	for i, column := range schema.Columns {
//		if i > 0 {
//			str.WriteString(",")
//		}
//		str.WriteString(fmt.Sprintf(" %s as %s %s", column.SourceName, column.ColumnName, column.Type))
//	}
//	str.WriteString(")")
//	return str.String(), nil
//}
