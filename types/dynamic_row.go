package types

import (
	"encoding/json"
	"fmt"
	"golang.org/x/exp/maps"
	"time"

	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
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
			l.OutputColumns[k] = v
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

//
//// TODO move all validation to the CLI https://github.com/turbot/tailpipe/issues/355
//// (one possible issue for this is that if a required field is missing and has not type, schema inference will fail - so maybe we also need to validate here)
//func (l *DynamicRow) Validate() error {
//	var missingFields []string
//	var invalidFields []string
//
//	// these fields are validated by CLI so we can ignore them
//	var excludedFromValidation = map[string]bool{
//		constants.TpIndex: true,
//		constants.TpDate:  true,
//	}
//
//	// only validate if there is no transform
//	var requiredFields []*schema.ColumnSchema
//
//	for _, column := range l.schema.Columns {
//		if column.Required && column.Transform == "" && !excludedFromValidation[column.ColumnName] {
//			requiredFields = append(requiredFields, column)
//		}
//	}
//	// Validate required fields
//	for _, column := range requiredFields {
//		val, ok := l.OutputColumns[column.ColumnName]
//		if !ok {
//			missingFields = append(missingFields, column.ColumnName)
//			continue
//		}
//
//		switch column.Type {
//		case "timestamp":
//			// if field is missing from output columns or invalid add to relevant collection
//			if err := l.validateTime(val); err != nil {
//				invalidFields = append(invalidFields, column.ColumnName)
//			}
//
//		default:
//			if val == "" {
//				missingFields = append(missingFields, column.ColumnName)
//			}
//			// TODO we need to move this to CLI as defaulting now happens there
//			// https://github.com/turbot/tailpipe/issues/364
//			// Special handling for tp_index - ensure lowercase
//			if column.ColumnName == constants.TpIndex {
//				l.OutputColumns[column.ColumnName] = strings.ToLower(val.(string))
//			}
//		}
//	}
//
//	if len(missingFields) > 0 || len(invalidFields) > 0 {
//		// return a RowErrorWithFields with the missing and invalid fields
//		return error_types.NewRowErrorWithFields(missingFields, invalidFields)
//	}
//
//	return nil
//}
//
//// validateTime validates the time field, return an error if time is missing or invalid
//func (l *DynamicRow) validateTime(t interface{}) error {
//	if t == nil {
//		return errors.New("time value is nil")
//	}
//	timeValue, ok := t.(time.Time)
//	if !ok {
//		return fmt.Errorf("time value is not a time.Time: %v", t)
//	}
//	if timeValue.IsZero() {
//		return errors.New("time value is zero")
//	}
//
//	return nil
//}

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.OutputColumns)
}

func (l *DynamicRow) GetSourceValue(s string) (string, bool) {
	v, ok := l.sourceColumns[s]
	return v, ok
}
