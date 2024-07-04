package schema

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
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
	Columns []*ColumnSchema
}

func (r *RowSchema) ToProto() *proto.Schema {
	var res = &proto.Schema{
		Columns: make([]*proto.ColumnSchema, len(r.Columns)),
	}

	for i, c := range r.Columns {
		pp := c.toProto()
		res.Columns[i] = pp
	}
	return res
}

func (r *RowSchema) Merge(other *RowSchema) {
	// add columns
	r.Columns = append(r.Columns, other.Columns...)
}

func RowSchemaFromProto(p *proto.Schema) *RowSchema {
	var res = &RowSchema{
		Columns: make([]*ColumnSchema, 0, len(p.Columns)),
	}
	for _, c := range p.Columns {
		res.Columns = append(res.Columns, ColumnFromProto(c))
	}
	return res
}

type ColumnSchema struct {
	SourceName string
	ColumnName string
	Type       string
	// for structs
	ChildFields []*ColumnSchema
}
type ColumnType struct {
	// DuckDB type`
	Type string
	// for structs/maps/struct[]
	ChildFields []*ColumnSchema
}

// toproto
func (c *ColumnSchema) toProto() *proto.ColumnSchema {
	p := &proto.ColumnSchema{
		SourceName: c.SourceName,
		ColumnName: c.ColumnName,
		Type:       c.Type,
	}
	for _, child := range c.ChildFields {
		p.ChildFields = append(p.ChildFields, child.toProto())
	}
	return p
}

// ColumnFromProto creates a new ColumnSchema from proto
func ColumnFromProto(p *proto.ColumnSchema) *ColumnSchema {
	c := &ColumnSchema{
		SourceName: p.SourceName,
		ColumnName: p.ColumnName,
		Type:       p.Type,
	}
	for _, child := range p.ChildFields {
		c.ChildFields = append(c.ChildFields, ColumnFromProto(child))
	}
	return c
}
