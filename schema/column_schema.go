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
	// SourceName refers to one of 2 things depdending on where the schema is used
	// 1. When the schemas is used by a mapper, SourceName refers to the field name in the raw row data
	// 2. When the schema is used by the JSONL conversion, SourceName refers to the column name in the JSONL
	SourceName string `json:"-"`
	ColumnName string `json:"name,omitempty"`
	// DuckDB type for the column
	Type string `json:"type"`
	// for struct and struct[]
	StructFields []*ColumnSchema `json:"struct_fields,omitempty"`
}

func (c *ColumnSchema) toProto() *proto.ColumnSchema {
	p := &proto.ColumnSchema{
		SourceName: c.SourceName,
		ColumnName: c.ColumnName,
		Type:       c.Type,
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
	//STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[]
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
		SourceName: p.SourceName,
		ColumnName: p.ColumnName,
		Type:       p.Type,
	}
	for _, child := range p.ChildFields {
		c.StructFields = append(c.StructFields, ColumnFromProto(child))
	}
	return c
}
