package schema

import (
	"errors"
	"fmt"
	"golang.org/x/exp/maps"
	"reflect"
	"strings"
	"time"

	"github.com/iancoleman/strcase"
)

const maxNesting = 10

func SchemaFromStruct(s any) (*RowSchema, error) {
	return NewSchemaBuilder().SchemaFromStruct(s)
}

type SchemaBuilder struct {
	typeMap map[reflect.Type]struct{}
	nesting int
}

func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		typeMap: make(map[reflect.Type]struct{}),
	}
}

func (b *SchemaBuilder) SchemaFromStruct(s any) (*RowSchema, error) {
	// Get the type of the rowStruct
	t := reflect.TypeOf(s)
	res, err := b.schemaFromType(t)
	if err != nil {
		return nil, err
	}
	// just use the column names from the struct, do not automap source fields
	res.AutoMapSourceFields = false

	// set the column descriptions
	commonFieldDescriptions := CommonFieldsColumnDescriptions()
	// if the struct implements GetColumnDescriptions, use this to populate the column descriptions
	if desc, ok := s.(GetColumnDescriptions); ok {
		columnDescriptions := desc.GetColumnDescriptions()
		// add in common field descriptions
		maps.Copy(columnDescriptions, commonFieldDescriptions)

		for _, c := range res.Columns {
			if desc, ok := columnDescriptions[c.ColumnName]; ok {
				c.Description = desc
			}
		}
	}

	return res, nil
}

func (b *SchemaBuilder) schemaFromType(t reflect.Type) (*RowSchema, error) {
	// if the type is a pointer, get the element type
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// check for circular deps
	if _, ok := b.typeMap[t]; ok {
		return nil, fmt.Errorf("circular reference detected")
	}
	b.typeMap[t] = struct{}{}
	defer delete(b.typeMap, t)

	// check for excessive recursion
	b.nesting++
	if b.nesting > maxNesting {
		return nil, fmt.Errorf("max recursion level %d reached", maxNesting)
	}
	defer func() {
		b.nesting--
	}()

	// reflect over parquet tags to build schema
	// build into map to avoid column name collisions (last column wins)

	// keep track of field order so schema columns reflects struct field order
	// (necessary as we use a map to avoid duplicate column names)
	idx := 0
	type schemaWithOrder struct {
		schema *ColumnSchema
		order  int
	}
	var res = map[string]schemaWithOrder{}

	var errorList []error

	// Iterate over the struct fields
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Get the parquet tag
		var p = &ParquetTag{}
		var err error

		c := &ColumnSchema{}

		// look for a parquet tag - this may override the name and/or type
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
			// set column name from the parquet tag if it is set
			if p.Name != "" {
				c.ColumnName = p.Name
			}
			// if the tag does not specify a type, infer from the field type
			if p.Type != "" {
				c.Type = p.Type
			}
		}

		// if the tag does not specify a type, infer from the field type
		if c.Type == "" {
			columnType, err := b.getColumnSchemaType(field.Type)
			if err != nil {
				errorList = append(errorList, fmt.Errorf("failed to get schema for field %s: %w", field.Name, err))
				continue
			}
			c.Type = columnType.Type
			c.StructFields = columnType.ChildFields
		}

		// if there is a JSON tag, use to populate the source name and the column name (if no parquet tag was found)
		if jsonTag := field.Tag.Get("json"); jsonTag != "" {
			split := strings.Split(jsonTag, ",")
			c.SourceName = split[0]
			if c.ColumnName == "" {
				c.ColumnName = split[0]
			}
		}

		// finally, if the column name is still empty, use the snake case of the field name
		if c.ColumnName == "" {
			c.ColumnName = strcase.ToSnake(field.Name)
		}
		if c.SourceName == "" {
			c.SourceName = field.Name
		}

		// if the field is an anonymous struct, MERGE the child fields into the parent
		if field.Anonymous && c.Type == "STRUCT" {
			for _, child := range c.StructFields {
				res[child.ColumnName] = schemaWithOrder{child, idx}
				idx++
			}
		} else {
			res[c.ColumnName] = schemaWithOrder{c, idx}
			idx++
		}
	}

	if len(errorList) > 0 {
		return nil, errors.Join(errorList...)
	}

	// now convert the map into a schema
	schema := &RowSchema{
		Columns: make([]*ColumnSchema, len(res)),
	}
	// construct the column array, respecting the order property
	for _, v := range res {
		schema.Columns[v.order] = v.schema
	}
	return schema, nil

}

func (b *SchemaBuilder) getColumnSchemaType(t reflect.Type) (ColumnType, error) {
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
		// TODO TACTICAL: the parquet conversion cannot handle struct arrays so treat as JSON
		// https://github.com/turbot/tailpipe-plugin-sdk/issues/55
		if isStruct(t.Elem()) {
			c.Type = "JSON"
			break
		}
		listType, err := b.getColumnSchemaType(t.Elem())
		if err != nil {
			return c, err
		}
		c.Type = fmt.Sprintf("%s[]", listType.Type)
		// for struct types, we need to wrap the child fields in a new ColumnSchema
		if listType.Type == "STRUCT" {
			c.ChildFields = listType.ChildFields
		}
	case reflect.Struct:
		// check if this is a time.Time
		if t == reflect.TypeOf(time.Time{}) {
			c.Type = "TIMESTAMP"
			break
		}
		// get the struct schema and convert into a DuckDB struct
		schema, err := b.schemaFromType(t)
		if err != nil {
			return c, err
		}
		// convert the schema into a DuckDB struct definition
		c.Type = "STRUCT"
		c.ChildFields = schema.Columns
	case reflect.Map:
		// TODO we do not currently support maps https://github.com/turbot/tailpipe-plugin-sdk/issues/55
		c.Type = "JSON"
	default:

		return c, fmt.Errorf("unsupported type %s", t)
	}
	return c, nil
}

func isStruct(elem reflect.Type) bool {
	return elem.Kind() == reflect.Struct || (elem.Kind() == reflect.Ptr && elem.Elem().Kind() == reflect.Struct)
}
