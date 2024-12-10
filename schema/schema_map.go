package schema

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"strings"
)

type SchemaMap map[string]*RowSchema

func (s SchemaMap) ToProto() map[string]*proto.Schema {
	var res = make(map[string]*proto.Schema, len(s))

	for k, v := range s {
		res[k] = v.ToProto()
	}
	return res

}

func SchemaMapFromProto(p map[string]*proto.Schema) SchemaMap {
	var res = make(SchemaMap, len(p))

	for k, v := range p {
		res[k] = RowSchemaFromProto(v)
	}
	return res
}

type RowSchema struct {
	Columns []*ColumnSchema `json:"columns"`
	// does this schema struct fully define the schema or is it partial
	Mode Mode `json:"schema_mode"`
}

func (r *RowSchema) ToProto() *proto.Schema {
	var res = &proto.Schema{
		Columns: make([]*proto.ColumnSchema, len(r.Columns)),
		Mode:    string(r.Mode),
	}

	for i, c := range r.Columns {
		pp := c.toProto()
		res.Columns[i] = pp
	}
	return res
}

func (r *RowSchema) DefaultTo(other *RowSchema) {
	// build a lookup of our columns
	var colLookup = make(map[string]*ColumnSchema, len(r.Columns))
	for _, c := range r.Columns {
		colLookup[c.SourceName] = c
	}

	for _, c := range other.Columns {
		// if we DO NOT already have this column, add it
		if _, ok := colLookup[c.SourceName]; !ok {
			r.Columns = append(r.Columns, c)
		}
	}
	// if the other schema is full, or our mode is dynamic, set our mode to whatever the other schema is
	if other.Mode == ModeFull || r.Mode == ModeDynamic {
		r.Mode = other.Mode
	}
}

func RowSchemaFromProto(p *proto.Schema) *RowSchema {
	var res = &RowSchema{
		Columns: make([]*ColumnSchema, 0, len(p.Columns)),
		Mode:    Mode(p.Mode),
	}
	for _, c := range p.Columns {
		res.Columns = append(res.Columns, ColumnFromProto(c))
	}
	return res
}

type ColumnSchema struct {
	SourceName string `json:"-"`
	ColumnName string `json:"name,omitempty"`
	// DuckDB type for the column
	Type string `json:"type"`
	// for struct and struct[]
	StructFields []*ColumnSchema `json:"struct_fields,omitempty"`
}

type ColumnType struct {
	// DuckDB type`
	Type string
	// for structs/maps/struct[]
	ChildFields []*ColumnSchema
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
