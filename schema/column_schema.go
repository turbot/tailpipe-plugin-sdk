package schema

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"strings"
)

type ColumnType struct {
	// DuckDB type`
	Type string
	// for structs/maps/struct[]
	ChildFields []*ColumnSchema
}

type ColumnSchema struct {
	// SourceName refers to the column name in the JSONL
	SourceName string
	// ColumnName refers to the column name in the parquet
	ColumnName string
	// DuckDB type for the column
	Type string
	// struct schema for for struct and struct[]
	StructFields []*ColumnSchema
	// the column description (optional)
	Description string
	// is the column required
	Required bool
	// The null value for the column
	NullValue string
	// The format of the time field so it can be recognized and analyzed properly.
	// Tailpipe uses strptime to parse time.
	// See the strptime documentation for available modifiers: https://linux.die.net/man/3/strptime
	TimeFormat string
	// a custom select clause for the column
	Transform string
}

func (c *ColumnSchema) toProto() *proto.ColumnSchema {
	p := &proto.ColumnSchema{
		SourceName:  c.SourceName,
		ColumnName:  c.ColumnName,
		Type:        c.Type,
		Description: c.Description,
		Required:    c.Required,
		NullValue:   c.NullValue,
		TimeFormat:  c.TimeFormat,
		Transform:   c.Transform,
	}
	for _, child := range c.StructFields {
		p.ChildFields = append(p.ChildFields, child.toProto())
	}
	return p
}

func (c *ColumnSchema) FullType() string {
	if c.Type == "STRUCT" {
		return c.structDef()
	}
	if c.Type == "STRUCT[]" {
		return fmt.Sprintf("%s[]", c.structDef())
	}
	return c.Type
}

func (c *ColumnSchema) structDef() string {
	var str strings.Builder
	str.WriteString("STRUCT(")
	for i, column := range c.StructFields {
		if i > 0 {
			str.WriteString(", ")
		}
		str.WriteString(fmt.Sprintf(`"%s" %s`, column.SourceName, column.FullType()))

	}
	str.WriteString(")")
	return str.String()
}

// ColumnFromProto creates a new ColumnSchema from proto
func ColumnFromProto(p *proto.ColumnSchema) *ColumnSchema {
	c := &ColumnSchema{
		SourceName:  p.SourceName,
		ColumnName:  p.ColumnName,
		Type:        p.Type,
		Description: p.Description,
		Required:    p.Required,
		NullValue:   p.NullValue,
		TimeFormat:  p.TimeFormat,
		Transform:   p.Transform,
	}
	for _, child := range p.ChildFields {
		c.StructFields = append(c.StructFields, ColumnFromProto(child))
	}
	return c
}
