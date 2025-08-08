package schema

import (
	"fmt"
	"path"
	"strings"

	"github.com/turbot/pipe-helpers/helpers"
	"github.com/turbot/pipe-helpers/utils"
	"github.com/turbot/tailpipe-plugin-sdk/error_types"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type TableSchema struct {
	Name    string
	Columns []*ColumnSchema
	// optional list of source columns match patterns to include in the table
	MapFields []string
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
		MapFields:   r.MapFields,
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
		MapFields:   p.MapFields,
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

	// NOTE: we DO NOT apply MapFields filtering here - we map all fields and let the CLI filter out source fields
	// which are not in the MapFields list
	// this is because any of the source fields may be required for a transform
	for k, v := range sourceMap {
		// check for null
		if r.NullIf != "" && v == r.NullIf {
			res[k] = nil
		} else {
			res[k] = v
		}
	}

	// now add all explicitly defined columns, IF they have a different source column mapped
	for _, c := range r.Columns {
		// if this field has a transform, do not map it
		if c.Transform != "" {
			continue
		}

		v, ok := sourceMap[c.SourceName]
		if !ok {
			// do not validate here - leave it to the validate function
			continue
		}

		// so we have a value for this column - is it null?
		if r.isNullValue(c, v) {
			// if the value matches the null string, set to nil
			res[c.ColumnName] = nil
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
		// attempt our 'smart' time parsing
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
	if c.NullIf != "" {
		nullValue = c.NullIf
	}
	return v == nullValue
}

// Complete checks if the types for all columns is known and that no source fields m,ust be mapped
// (if any types are unknown or any source fields are being mapped, we need to infer the full schema once we have some source data)
func (r *TableSchema) Complete() bool {
	return len(r.columnsWithNoType()) == 0 && len(r.MapFields) == 0
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
// NOTE: this is the same validation as we perform in tailpipe Table.Validate - that validates the TableDef in config,
// whereas as this validates the hardcoded TableDef provided by the plugin
func (r *TableSchema) Validate() error {
	var optionalColumnsWithNoType []string
	for _, c := range r.Columns {
		// validate the column and its struct fields
		if err := r.validateColumn(c, r.Name); err != nil {
			return err
		}

		if !c.Required && c.Type == "" {
			optionalColumnsWithNoType = append(optionalColumnsWithNoType, c.ColumnName)
		}
	}

	if len(optionalColumnsWithNoType) > 0 {
		return fmt.Errorf("column type must be specified if column is optional (%s '%s')", utils.Pluralize("column", len(optionalColumnsWithNoType)), strings.Join(optionalColumnsWithNoType, "', '"))
	}
	return nil
}

// validateColumn validates a single column and its struct fields recursively
func (r *TableSchema) validateColumn(c *ColumnSchema, tableName string) error {
	// if no source is specified, use the column name
	if c.SourceName == "" && c.Transform == "" {
		c.SourceName = c.ColumnName
	}

	// validate struct fields recursively
	for _, sf := range c.StructFields {
		// if no source is specified, use the column name
		if sf.SourceName == "" && sf.Transform == "" {
			sf.SourceName = sf.ColumnName
		}
		// validate the struct field type
		if sf.Type != "" {
			// special case - struct arrays not supported
			if strings.ToLower(sf.Type) == "struct[]" || !IsValidColumnType(sf.Type) {
				return fmt.Errorf("invalid column type '%s' for struct field '%s' in column '%s' in table '%s'", sf.Type, sf.ColumnName, c.ColumnName, tableName)
			}
		}
	}

	// validate the column type
	if c.Type != "" {
		// special case cannot specify a struct in a tag
		if strings.ToLower(c.Type) == "struct[]" || !IsValidColumnType(c.Type) {
			return fmt.Errorf("invalid column type '%s' for column '%s' in table '%s'", c.Type, c.ColumnName, tableName)
		}
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
		MapFields:   r.MapFields,
		Description: r.Description,
		NullIf:      r.NullIf,
	}

	// Copy our columns
	for i, col := range r.Columns {
		merged.Columns[i] = col.Clone()
	}
	return merged
}

// WithSourceFieldsCleared returns a copy with the source fields set to the column names
// this is called from RowEnrichmentCollector as it will already have applied field mappings
func (r *TableSchema) WithSourceFieldsCleared() *TableSchema {
	cloned := r.Clone()

	for i, c := range cloned.Columns {
		// set the source name to the column name - we have already mapped the source column
		c.SourceName = c.ColumnName
		// clear the transform as we have already applied it
		c.Transform = ""
		cloned.Columns[i] = c
		for j, sf := range c.StructFields {
			// set the source name to the column name
			sf.SourceName = sf.ColumnName
			c.StructFields[j] = sf
		}
	}
	return cloned
}

// NormaliseColumnTypes normalises the column types to lower case
func (r *TableSchema) NormaliseColumnTypes() {
	for _, c := range r.Columns {
		c.NormaliseColumnTypes()
	}
}

func (r *TableSchema) ShouldMapSourceColumn(columnName string) bool {
	for _, mapField := range r.MapFields {
		if matches, _ := path.Match(mapField, columnName); matches {
			return true
		}
	}
	return false
}
