package schema

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// SchemaMap is a map of table names to RowSchema
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
