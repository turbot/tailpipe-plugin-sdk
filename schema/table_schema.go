package schema

import (
	"fmt"
	"strings"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type TableSchema struct {
	Name    string          `json:"name,omitempty"`
	Columns []*ColumnSchema `json:"columns"`
	// should we include ALL source fields in addition to any defined columns, or ONLY include the columns defined
	AutoMapSourceFields bool `json:"automap_source_fields"`
	// should we exclude any source fields from the output (only applicable if automap_source_fields is true)
	ExcludeSourceFields []string `json:"exclude_source_fields"`
	// is this a custom table - this inkudes 'predefined' custom tables which use the custom table mechanism to define
	// a fixed table - such as nginx access logs
	// this is used when building the select query - we use a different query for custom tables
	CustomTable bool `json:"custom_table"`

	// the table description (optional)
	Description string `json:"description,omitempty"`
	// the default null value for the table (may be overriden for specific columns
	NullValue string `json:"null_value,omitempty"`
}

func (r *TableSchema) ToProto() *proto.Schema {
	var res = &proto.Schema{
		Name:                r.Name,
		Columns:             make([]*proto.ColumnSchema, len(r.Columns)),
		AutomapSourceFields: r.AutoMapSourceFields,
		ExcludeSourceFields: r.ExcludeSourceFields,
		Description:         r.Description,
		NullValue:           r.NullValue,
		CustomTable:         r.CustomTable,
	}

	for i, c := range r.Columns {
		pp := c.toProto()
		res.Columns[i] = pp
	}
	return res
}

func (r *TableSchema) AsMap() map[string]*ColumnSchema {
	var res = make(map[string]*ColumnSchema, len(r.Columns))
	for _, c := range r.Columns {
		res[c.ColumnName] = c
	}
	return res
}

func TableSchemaFromProto(p *proto.Schema) *TableSchema {
	var res = &TableSchema{
		Name:                p.Name,
		Columns:             make([]*ColumnSchema, 0, len(p.Columns)),
		AutoMapSourceFields: p.AutomapSourceFields,
		ExcludeSourceFields: p.ExcludeSourceFields,
		Description:         p.Description,
		NullValue:           p.NullValue,
		CustomTable:         p.CustomTable,
	}
	for _, c := range p.Columns {
		res.Columns = append(res.Columns, ColumnFromProto(c))
	}
	return res
}

// MapRow maps a row from a map of source fields to a map of target fields, applying the schema
// and respecting the automap and exclude fields
func (r *TableSchema) MapRow(rowMap map[string]string) (map[string]string, error) {
	var res = make(map[string]string, len(r.Columns))

	schemaMap := r.AsMap()

	if r.AutoMapSourceFields {
		// build map of excluded fields
		excludeMap := utils.SliceToLookup(r.ExcludeSourceFields)
		for k, v := range rowMap {
			// if. this field is NOT excluded, and we do not have a schema for it, add it to the result as is
			_, exclude := excludeMap[k]
			_, haveSchema := schemaMap[k]

			if !exclude && !haveSchema {
				// just set the value
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
		//
		if v, ok := rowMap[sourceName]; !ok {
			if c.Required {
				return nil, fmt.Errorf("source field '%s' not found in row", sourceName)
			}
			// if the field is not required, we just skip it
		} else {
			// check for null value
			// by default, treat an empty string as a null value, but this may be overridden by the config
			if !r.isNullValue(c, v) {

				// map the value - this handles nulls and correct formatting arrays and times
				val, err := r.mapValue(c, v)
				if err != nil {
					return nil, err
				}
				res[c.ColumnName] = val
			}
		}
	}
	return res, nil
}

func (r *TableSchema) mapValue(column *ColumnSchema, valString string) (string, error) {
	ty := column.Type

	//// treat arrays separately
	//if arrayType, isArray := strings.CutSuffix(ty, "[]"); isArray{
	//	return  mapArrayValue(valString, arrayType)
	//}

	// now format the string according to the type
	switch ty {
	case "TIMESTAMP", "DATE", "TIME":
		t, err := helpers.ParseTime(valString)
		if err != nil {
			return valString, fmt.Errorf("error parsing time value '%s' for column '%s': %w", valString, column.ColumnName, err)
		}
		// format the time as a string
		return t.Format(time.RFC3339), nil
	default:
		return valString, nil
	}
}

//func mapArrayValue(valString, ty string) (any, error) {
//	var res []any
//	// TODO should we split on commas and trim spaces? https://github.com/turbot/tailpipe-plugin-sdk/issues/102
//	switch ty {
//	case "TIMESTAMP", "DATE", "TIME":
//		t, err := helpers.ParseTime(valString)
//		if err != nil {
//			return "", err
//		}
//		// format the time as a string
//		res = append(res, t.Format(time.RFC3339))
//	default:
//		res = append(res, valString)
//
//	}
//
//	return fmt.Sprintf("[%s]", valString), nil
//}

// InitialiseFromInferredSchema populates this schema using an inferred row schema
// this is called from the CLI when we are trying to determine the full schema after receiving the first JSONL file
// it either adds all fields in the inferred schema (if AutoMapSourceFields is true) or
// just populate missing types if AutoMapSourceFields is false
func (r *TableSchema) InitialiseFromInferredSchema(inferredSchema *TableSchema) {
	// TODO test this https://github.com/turbot/tailpipe/issues/108
	// if we are in autoMap mode, we use the inferred schema in full
	if r.AutoMapSourceFields {
		// store our own schema as a map
		selfMap := r.AsMap()
		excludedMap := utils.SliceToLookup(r.ExcludeSourceFields)
		for _, c := range inferredSchema.Columns {
			// skip common fields (which will already be in our schema)
			if IsCommonField(c.ColumnName) {
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
		// we are not automapping - just the type for any columns missing a type
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
}

func (r *TableSchema) isNullValue(c *ColumnSchema, v string) bool {
	nullValue := r.NullValue
	if c.NullValue != "" {
		nullValue = c.NullValue
	}
	return v == nullValue
}

func (r *TableSchema) Complete() bool {
	return len(r.columnsWithNoType()) == 0 && !r.AutoMapSourceFields
}

func (r *TableSchema) columnsWithNoType() []string {
	var res []string
	for _, c := range r.Columns {
		if c.Type == "" {
			res = append(res, c.ColumnName)
		}
	}
	return res
}

// Validate checks that all optional columns have a type and returns an error if not
// The purpose of this function is to validate the TableDefinition provided by a 'predefined custom table'
// This validation ensures that any optional columns have a type specified, so we can correctly create the parquet schema
// even if the column is not present in the source data
// NOTE: this is the same validation as we perform in tailpipe Table.Validate - that validfates the TableDef in config,
// whereas as this validates the hardcoded TableDef provided by the plugin
func (r *TableSchema) Validate() error {
	var optionalColumnsWithNoType []string
	for _, c := range r.Columns {
		if !c.Required && c.Type == "" {
			optionalColumnsWithNoType = append(optionalColumnsWithNoType, c.ColumnName)
		}
	}

	if len(optionalColumnsWithNoType) > 0 {
		return fmt.Errorf("column type must be specified if column is optional (%s '%s')", utils.Pluralize("column", len(optionalColumnsWithNoType)), strings.Join(optionalColumnsWithNoType, "', '"))
	}
	return nil
}

// EnsureComplete checks that all columns have a type and returns an error if not
func (r *TableSchema) EnsureComplete() error {
	// verify all columns have a type
	missingTypes := r.columnsWithNoType()
	if len(missingTypes) > 0 {
		return fmt.Errorf("it was not possible to infer types for all columns - please check the table definition: %s", strings.Join(missingTypes, ", "))
	}
	return nil
}

// MergeWithCommonSchema merges the table schema with the common fields schema
// if this schema contains definitions for any common fields, the only thing that will be used is the source name
func (r *TableSchema) MergeWithCommonSchema() *TableSchema {
	// get the common fields schema
	commonFieldsSchema := CommonFieldsSchema()

	// create a new schema our top level properties and the common fields - we will add our columns next
	var merged = &TableSchema{
		Name:                r.Name,
		Columns:             commonFieldsSchema.Columns,
		AutoMapSourceFields: r.AutoMapSourceFields,
		ExcludeSourceFields: r.ExcludeSourceFields,
		Description:         r.Description,
		NullValue:           r.NullValue,
	}

	commonFieldsMap := merged.AsMap()

	for _, c := range r.Columns {
		// for the common fields, set the type and required from
		if commonColumn, ok := commonFieldsMap[c.ColumnName]; ok {
			// mutate the common column in the merged schema
			commonColumn.SourceName = c.SourceName
			commonColumn.TimeFormat = c.TimeFormat
			commonColumn.SelectClause = c.SelectClause
			continue
		}

		merged.Columns = append(merged.Columns, &ColumnSchema{
			ColumnName: c.ColumnName,
			// NOTE: do not set the source from the table schema - just use the column name
			// - the source in the table config relates to the mapping from raw row to mapped rown
			// this schema will be used to convert the JSONL (i.e. the mapped row) to parquet
			SourceName: c.ColumnName,
			Type:       c.Type,
			Required:   c.Required,
		})
	}

	merged.AutoMapSourceFields = r.AutoMapSourceFields
	return merged
}

// TODO TACTICAL
// return a copy with the source fields set the the fcolumn names - this is used to create the parquet schema
// SourceName refers to one of 2 things depdending on where the schema is used
// 1. When the schemas is used by a mapper, SourceName refers to the field name in the raw row data
// 2. When the schema is used by the JSONL conversion, SourceName refers to the column name in the JSONL

func (r *TableSchema) WithSourceFieldsCleared() *TableSchema {
	res := &TableSchema{
		Name:                r.Name,
		Columns:             make([]*ColumnSchema, len(r.Columns)),
		AutoMapSourceFields: r.AutoMapSourceFields,
		ExcludeSourceFields: r.ExcludeSourceFields,
		Description:         r.Description,
		NullValue:           r.NullValue,
	}

	for i, c := range r.Columns {
		res.Columns[i] = &ColumnSchema{
			ColumnName: c.ColumnName,
			SourceName: c.ColumnName,
			Type:       c.Type,
			Required:   c.Required,
		}
	}
	return res
}
