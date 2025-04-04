package schema

import (
	"fmt"
	"strings"

	"github.com/danwakefield/fnmatch"
	"github.com/itchyny/timefmt-go"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/error_types"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type TableSchema struct {
	Name    string
	Columns []*ColumnSchema
	// optional pattern to match source fields to include in the schema
	Select string
	// the table description (optional)
	Description string
	// the default null value for the table (may be overridden for specific columns)
	NullIf string
}

func (r *TableSchema) ToProto() *proto.Schema {
	var res = &proto.Schema{
		Name:        r.Name,
		Columns:     make([]*proto.ColumnSchema, len(r.Columns)),
		Description: r.Description,
		NullValue:   r.NullIf,
		Select:      r.Select,
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
		Name:        p.Name,
		Columns:     make([]*ColumnSchema, 0, len(p.Columns)),
		Select:      p.Select,
		Description: p.Description,
		NullIf:      p.NullValue,
	}
	for _, c := range p.Columns {
		res.Columns = append(res.Columns, ColumnFromProto(c))
	}
	return res
}

// MapRow maps a row from a map of source fields to a map of target fields, applying the schema
// and respecting the automap and exclude fields
func (r *TableSchema) MapRow(sourceMap map[string]string) (map[string]interface{}, error) {
	var missingFields []string
	var invalidFields []string

	var res = make(map[string]interface{}, len(r.Columns))

	schemaMap := r.AsMap()

	// do we have a pattern for selecting source fields? If not, exclude them all
	if r.Select != "" {
		for k, v := range sourceMap {
			// does this column match the pattern?
			matchPattern := fnmatch.Match(r.Select, k, fnmatch.FNM_IGNORECASE)
			// do we already have a schema for this column?
			_, haveSchema := schemaMap[k]
			// is the value null?
			isNull := r.NullIf != "" && v == r.NullIf

			// should we include this source value?
			if matchPattern && !haveSchema && !isNull {
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
		v, ok := sourceMap[sourceName]
		if !ok {
			if c.Required {
				// TODO: #error think about this more since technically it's the source that is missing but we are returning the column name
				// TODO: #error consider a separate mapping error with multiple fields (source/dest)
				// if the field is required, add it to the missing fields
				missingFields = append(missingFields, c.ColumnName)
			}
			// if the field is not required, we just skip it
			continue
		}

		// so we have a value for this column - is it null?
		if r.isNullValue(c, v) {
			// if the value matches the null string, skip it - it will appear as null in the parquet
			continue
		}

		// so we have a non null value

		// map the value - this handles type conversion for arrays and time, and applying custom transforms
		val, err := r.mapValue(c, v)
		if err != nil {
			// if we have an error mapping the value, add the column to the invalid fields
			invalidFields = append(invalidFields, c.ColumnName)
			continue
		}
		// map value to column
		res[c.ColumnName] = val
	}

	if len(missingFields) > 0 || len(invalidFields) > 0 {
		return nil, error_types.NewRowErrorWithFields(missingFields, invalidFields)
	}

	return res, nil
}

func (r *TableSchema) mapValue(column *ColumnSchema, valString string) (interface{}, error) {
	ty := column.Type
	// now format the string according to the type
	switch ty {
	case "timestamp", "date", "time":
		// if a time format was specified, attempt to parse the value using that format
		if column.TimeFormat != "" {
			return timefmt.Parse(valString, column.TimeFormat)
		}
		// otherwise attempt out 'smart' time parsing
		t, err := helpers.ParseTime(valString)
		if err != nil {
			return valString, fmt.Errorf("error parsing time value '%s' for column '%s': %w", valString, column.ColumnName, err)
		}
		return t, nil

	default:

		// if it is an array, treat as a single value in an array
		// if it needs splitting, the config should specify a select clause
		if strings.HasSuffix(ty, "[]") {
			// return as a slice
			return []any{valString}, nil
		}

		// for all other types, just return the string and rely on
		return valString, nil
	}
}

func (r *TableSchema) isNullValue(c *ColumnSchema, v string) bool {
	// TODO KAI check default
	nullValue := r.NullIf
	if c.NullValue != "" {
		nullValue = c.NullValue
	}
	return v == nullValue
}

func (r *TableSchema) Complete() bool {
	return len(r.columnsWithNoType()) == 0 && r.Select == ""
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
	merged := r.Clone()

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
			merged.Columns = append(merged.Columns, commonCol.Clone())
		}
	}

	return merged
}

func (r *TableSchema) Clone() *TableSchema {
	merged := &TableSchema{
		Name:        r.Name,
		Columns:     make([]*ColumnSchema, len(r.Columns)),
		Select:      r.Select,
		Description: r.Description,
		NullIf:      r.NullIf,
	}

	// Copy our columns
	for i, col := range r.Columns {
		merged.Columns[i] = col.Clone()
	}
	return merged
}

// WithSourceFieldsCleared returns a copy with the source fields set the the fcolumn names - this is used to create the parquet schema
// SourceName refers to one of 2 things depdending on where the schema is used
// 1. When the schemas is used by a mapper, SourceName refers to the field name in the raw row data
// 2. When the schema is used by the JSONL conversion, SourceName refers to the column name in the JSONL
func (r *TableSchema) WithSourceFieldsCleared() *TableSchema {
	cloned := r.Clone()

	for i, c := range cloned.Columns {
		// set the source name to the column name
		c.SourceName = c.ColumnName
		cloned.Columns[i] = c
	}
	return cloned
}

// NormaliseColumnTypes normalises the column types to lower case
func (r *TableSchema) NormaliseColumnTypes() {
	for _, c := range r.Columns {
		c.NormaliseColumnTypes()
	}
}
