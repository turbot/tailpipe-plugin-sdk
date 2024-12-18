package schema

import (
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
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
	// if the other schema is NOT set to automap source fields, we should not either
	if other.AutoMapSourceFields == false {
		r.AutoMapSourceFields = false
	}
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
