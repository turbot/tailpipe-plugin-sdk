package types

import (
	"encoding/json"
	"fmt"
	"golang.org/x/exp/maps"
	"strings"
	"time"

	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/error_types"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DynamicRow struct {
	// the source columns as a string map (the format output by the mappers)
	sourceColumns map[string]string

	// the output columns, as a map of string to interface{} - the result of enrichment and type conversion
	OutputColumns map[string]interface{}

	schema *schema.TableSchema
}

func (l *DynamicRow) InitialiseFromMap(m map[string]string) error {
	// just assign the source columns
	l.sourceColumns = m
	l.OutputColumns = make(map[string]interface{})
	return nil
}

// Enrich uses the provided mappings to populate the common fields from mapped column values
func (l *DynamicRow) Enrich(tableSchema *schema.TableSchema, sourceEnrichmentFields schema.SourceEnrichment) error {
	// store the schema - we will use in validation
	l.schema = tableSchema

	// we expect the columns to be initialised by a previous call to InitialiseFromMap
	if l.sourceColumns == nil {
		// pass this back as a normal error, it should be converted to a RowErrorWithMessage by a caller which can populate the source
		return fmt.Errorf("the DynamicRow struct has not been initialised with a map of columns")
	}

	// merge in source common fields
	// NOTE: these have precedence over any source related tp columns which are already populated
	// from the source data - this is by design
	for k, v := range sourceEnrichmentFields.CommonFields.AsMap() {
		// if there a non empty value for this field, include it
		if v != "" {
			l.sourceColumns[k] = v
		}
	}

	// now ask the schema to map the row for uas
	outputColumns, err := tableSchema.MapRow(l.sourceColumns)
	if err != nil {
		// err will be error_types.RowErrorWithFields
		return err
	}
	// merge the output columns with our current output columns, with the rows current out columns having precedence
	// (the plugin may have added some columns to the row)
	maps.Copy(outputColumns, l.OutputColumns)
	l.OutputColumns = outputColumns

	// auto populate id and ingest timestamp
	l.OutputColumns[constants.TpID] = xid.New().String()
	l.OutputColumns[constants.TpIngestTimestamp] = time.Now()

	return nil
}

// TODO move all validation to the CLI https://github.com/turbot/tailpipe/issues/355
func (l *DynamicRow) Validate() error {
	var missingFields []string
	var invalidFields []string

	//
	// Define time fields that need validation
	requiredTimeFields := []string{
		constants.TpIngestTimestamp,
		constants.TpTimestamp,
		// this is added by CLI
		//constants.TpDate,
	}

	// Define required string fields
	requiredStringFields := []string{
		constants.TpID,
		constants.TpSourceType,
		constants.TpTable,
		constants.TpPartition,
		// this is added by CLI
		//constants.TpIndex,
	}

	// can we validate this row?
	// if any required fields have transform functions, we cannot validate at this point
	// - we must wait until after the transform has been executed by the CLI - the CLI will do the validation
	requiredFields := append(requiredStringFields, requiredTimeFields...)
	schemaMap := l.schema.AsMap()
	for _, field := range requiredFields {
		if schemaMap[field].Transform != "" {
			// this field has a transform function - we cannot validate at this point
			return nil
		}
	}

	// OK so we can validate
	// Validate time fields
	for _, field := range requiredTimeFields {
		// if field is missing from output columns or invalid add to relevant collection
		missing, invalid := l.validateTime(l.OutputColumns[field])
		if missing {
			missingFields = append(missingFields, field)
		}
		if invalid {
			invalidFields = append(invalidFields, field)
		}
	}

	// Special validation for tp_date to ensure it's a date without time component
	if dateVal, ok := l.OutputColumns[constants.TpDate].(string); ok && dateVal != "" {
		if parsedDate, err := time.Parse(time.RFC3339, dateVal); err == nil {
			if !parsedDate.Equal(parsedDate.Truncate(24 * time.Hour)) {
				invalidFields = append(invalidFields, constants.TpDate)
			}
		}
	}

	// Validate required string fields
	for _, field := range requiredStringFields {
		val, ok := l.OutputColumns[field].(string)
		if !ok || val == "" {
			missingFields = append(missingFields, field)
			continue
		}
		// Special handling for tp_index - ensure lowercase
		if field == constants.TpIndex {
			l.OutputColumns[field] = strings.ToLower(val)
		}
	}

	if len(missingFields) > 0 || len(invalidFields) > 0 {
		// return a RowErrorWithFields with the missing and invalid fields
		return error_types.NewRowErrorWithFields(missingFields, invalidFields)
	}

	return nil
}

// validateTime validates the time field returning two bools
// - the first bool is true if the time is missing
// - the second bool is true if the time is invalid
func (l *DynamicRow) validateTime(t interface{}) (missing, invalid bool) {
	if t == nil {
		return true, false
	}
	timeValue, ok := t.(time.Time)
	if !ok {
		return false, true
	}
	if timeValue.IsZero() {
		return true, false
	}

	return false, false
}

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.OutputColumns)
}

func (l *DynamicRow) GetSourceValue(s string) (string, bool) {
	v, ok := l.sourceColumns[s]
	return v, ok
}
