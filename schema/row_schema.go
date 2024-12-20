package schema

import (
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type RowSchema struct {
	Columns []*ColumnSchema `json:"columns"`
	// should we include ALL source fields in addition to any defined columns, or ONLY include the columns defined
	AutoMapSourceFields bool `json:"automap_source_fields"`
	// should we exclude any source fields from the output (only applicable if automap_source_fields is true)
	ExcludeSourceFields []string `json:"exclude_source_fields"`
}

func (r *RowSchema) ToProto() *proto.Schema {
	var res = &proto.Schema{
		Columns:             make([]*proto.ColumnSchema, len(r.Columns)),
		AutomapSourceFields: r.AutoMapSourceFields,
		ExcludeSourceFields: r.ExcludeSourceFields,
	}

	for i, c := range r.Columns {
		pp := c.toProto()
		res.Columns[i] = pp
	}
	return res
}

func (r *RowSchema) AsMap() map[string]*ColumnSchema {
	var res = make(map[string]*ColumnSchema, len(r.Columns))
	for _, c := range r.Columns {
		res[c.ColumnName] = c
	}
	return res
}

func RowSchemaFromProto(p *proto.Schema) *RowSchema {
	var res = &RowSchema{
		Columns:             make([]*ColumnSchema, 0, len(p.Columns)),
		AutoMapSourceFields: p.AutomapSourceFields,
		ExcludeSourceFields: p.ExcludeSourceFields,
	}
	for _, c := range p.Columns {
		res.Columns = append(res.Columns, ColumnFromProto(c))
	}
	return res
}

func (r *RowSchema) MapRow(rowMap map[string]string) (map[string]string, error) {
	var res = make(map[string]string, len(r.Columns))

	if r.AutoMapSourceFields {
		// build map of excluded fields
		excludeMap := utils.SliceToLookup(r.ExcludeSourceFields)
		for k, v := range rowMap {
			// if. this field is NOT excluded, add it to the result
			if _, ok := excludeMap[k]; !ok {
				res[k] = v
			}
		}
	}

	// now add all explicitly defined columns
	for _, c := range r.Columns {
		sourceName := c.ColumnName
		if c.SourceName != "" {
			sourceName = c.SourceName
		}
		if v, ok := rowMap[sourceName]; !ok {
			// TODO once we have config for this, we can decide if this is an error or not
			return nil, fmt.Errorf("source field %s not found in row", sourceName)
		} else {
			res[c.ColumnName] = v
		}
	}
	return res, nil
}

// InitialiseFromInferredSchema populates this schema using an inferred row schema
// this is called from the CLI when we are trying to determine the full schema after receiving the first JSONL file
// it either adds all fields in the inferred schema (if AutoMapSourceFields is true) or
// just populate missing types if AutoMapSourceFields is false
func (r *RowSchema) InitialiseFromInferredSchema(inferredSchema *RowSchema) {
	// TOPDO K test this
	// if we are in autoMap mode, we use the inferred schema in full
	if r.AutoMapSourceFields {
		// store our own schema as a map
		selfMap := r.AsMap()
		excludedMap := utils.SliceToLookup(r.ExcludeSourceFields)
		for _, c := range inferredSchema.Columns {
			// skip common fields (which will already be in our schema)
			if enrichment.IsCommonField(c.ColumnName) {
				continue
			}
			// skip any excluded fields
			if _, excluded := excludedMap[c.ColumnName]; excluded {
				continue
			}
			// we already have this column - does it have a type?
			if columnSchema, haveColumn := selfMap[c.ColumnName]; haveColumn {
				if columnSchema.Type == "" {
					columnSchema.Type = c.Type
				}
			} else {
				// we do not have this column - add it add this column
				r.Columns = append(r.Columns, c)
			}
		}
	} else {
		// we ar not automapping - just the typ efor any columns missing a type
		inferredMap := inferredSchema.AsMap()

		for _, c := range r.Columns {
			if c.Type == "" {
				columnSchema, ok := inferredMap[c.ColumnName]
				if !ok {
					return
				}
				c.Type = columnSchema.Type
			}
		}
	}
	return
}

func (r *RowSchema) Complete() bool {
	return len(r.columnsWithNoType()) == 0 && !r.AutoMapSourceFields
}

func (r *RowSchema) columnsWithNoType() []string {
	var res []string
	for _, c := range r.Columns {
		if c.Type == "" {
			res = append(res, c.ColumnName)
		}
	}
	return res
}
