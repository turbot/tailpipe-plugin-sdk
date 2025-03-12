package schema

import (
	"database/sql"
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
	}
	for _, c := range p.Columns {
		res.Columns = append(res.Columns, ColumnFromProto(c))
	}
	return res
}

// MapRow maps a row from a map of source fields to a map of target fields, applying the schema
// and respecting the automap and exclude fields
func (r *TableSchema) MapRow(sourceMap map[string]string) (map[string]interface{}, error) {
	var res = make(map[string]interface{}, len(r.Columns))

	schemaMap := r.AsMap()

	if r.AutoMapSourceFields {
		// build map of excluded fields
		excludeMap := utils.SliceToLookup(r.ExcludeSourceFields)
		for k, v := range sourceMap {
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
		// default source name to column name
		sourceName := c.ColumnName
		if c.SourceName != "" {
			sourceName = c.SourceName
		}
		//
		if v, ok := sourceMap[sourceName]; !ok {
			if c.Required {
				return nil, fmt.Errorf("column '%s' is required, but source field '%s' not found in row", c.ColumnName, sourceName)
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

func (r *TableSchema) mapValue(column *ColumnSchema, valString string) (interface{}, error) {
	ty := column.Type

	//// treat arrays separately
	//if arrayType, isArray := strings.CutSuffix(ty, "[]"); isArray{
	//	return  mapArrayValue(valString, arrayType)
	//}
	// todo use duckdb to map

	// if a select clause is provided, use that
	if column.SelectClause != "" {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return "", fmt.Errorf("error opening duckdb connection: %w", err)
		}
		defer db.Close()
		// use the select clause to map the value
		// we assume (and validate) that the select clause a DuckDB function name, with a parameter, e.g. 'UPPER(?)'
		// TODO verify the select clause contains 1 param '?'

		query := fmt.Sprintf("SELECT %s", column.SelectClause)
		row := db.QueryRow(query, valString)
		var val interface{}
		err = row.Scan(&val)
		if err != nil {
			return "", fmt.Errorf("error executing select clause '%s' for column '%s': %w", column.SelectClause, column.ColumnName, err)
		}
		return val, nil
	}
	// if the type is a date time, parse it

	// now format the string according to the type
	switch ty {
	case "TIMESTAMP", "DATE", "TIME":
		t, err := helpers.ParseTime(valString)
		if err != nil {
			return valString, fmt.Errorf("error parsing time value '%s' for column '%s': %w", valString, column.ColumnName, err)
		}
		// format the time as a string
		return t.Format(time.RFC3339), nil
	//	TODO array, struct
	default:

		// if it is an array of any kind, default to split on commas
		if strings.HasSuffix(ty, "[]") {
			vals := strings.Split(valString, ",")
			// trim spaces
			for i, v := range vals {
				vals[i] = strings.TrimSpace(v)
			}
			// return as a slice
			return vals, nil
		}

		// for all other types, just return the string and rely on
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

// MergeWithCommonSchema merges the table schema with the common fields schema.
// The resulting schema will contain:
// - All fields from this schema
// - For common fields, Type and Required are taken from the common schema, and Description if not already set
// - Any common fields not in this schema are added
// The original schema is not modified.
func (r *TableSchema) MergeWithCommonSchema() *TableSchema {
	// Get the common fields schema
	commonFieldsSchema := CommonFieldsSchema()
	if commonFieldsSchema == nil {
		// Defensive programming - should never happen but just in case
		return r
	}

	// Start with a copy of our schema
	merged := &TableSchema{
		Name:                r.Name,
		Columns:             make([]*ColumnSchema, len(r.Columns)),
		AutoMapSourceFields: r.AutoMapSourceFields,
		ExcludeSourceFields: r.ExcludeSourceFields,
		Description:         r.Description,
		NullValue:           r.NullValue,
	}

	// Copy our columns
	for i, col := range r.Columns {
		merged.Columns[i] = &ColumnSchema{
			ColumnName:  col.ColumnName,
			SourceName:  col.SourceName,
			Type:        col.Type,
			Required:    col.Required,
			Description: col.Description,
			NullValue:   col.NullValue,
		}
	}

	// Create map for efficient lookup
	mergedMap := merged.AsMap()

	// Process common fields
	for _, commonCol := range commonFieldsSchema.Columns {
		if existingCol, exists := mergedMap[commonCol.ColumnName]; exists {
			// Column exists - always use Type and Required from common schema
			existingCol.Type = commonCol.Type
			existingCol.Required = commonCol.Required
			// Set Description only if not already set
			if existingCol.Description == "" {
				existingCol.Description = commonCol.Description
			}
			// Set SourceName only if not already set
			if existingCol.SourceName == "" {
				existingCol.SourceName = commonCol.SourceName
			}
		} else {
			// Column doesn't exist - add the common column
			merged.Columns = append(merged.Columns, commonCol)
		}
	}

	return merged
}

// WithSourceFieldsCleared returns a copy with the source fields set the the fcolumn names - this is used to create the parquet schema
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
