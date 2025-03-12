package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/xid"
	"golang.org/x/exp/maps"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"

	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DynamicRow struct {
	// the source columns as a string map (the format output by the mappers)
	sourceColumns map[string]string

	// the output columns, as a map of string to interface{} - the result of enrichment and type conversion
	OutputColumns map[string]interface{}
}

func (l *DynamicRow) InitialiseFromMap(m map[string]string) error {
	// just assign the source columns
	l.sourceColumns = m
	l.OutputColumns = make(map[string]interface{})
	return nil
}

// Enrich uses the provided mappings to populate the common fields from mapped column values
func (l *DynamicRow) Enrich(tableSchema *schema.TableSchema, sourceEnrichmentFields schema.SourceEnrichment) error {
	// we expect the columns to be initialised by a previous call to InitialiseFromMap
	if l.sourceColumns == nil {
		return fmt.Errorf("the DynamicRow struct has not been initialised with a map of columns")
	}

	// merge in source common fields
	// TODO - when CommonFields.AsMap returns map[string]any, we can apply this directly to OutputColumns
	// NOTE: these have precedence over any source related tp columns which are already populated
	// from the source data - this is by design
	for k, v := range sourceEnrichmentFields.CommonFields.AsMap() {
		if _, ok := l.sourceColumns[k]; !ok {
			l.sourceColumns[k] = v
		}
	}

	// now ask the schema to map the row for uas
	outputColumns, err := tableSchema.MapRow(l.sourceColumns)
	if err != nil {
		return fmt.Errorf("error mapping row: %w", err)
	}
	// merge the output columns with our current output columns, with the rows current out columns having precedence
	// (the plugin may have added some columns to the row)
	maps.Copy(outputColumns, l.OutputColumns)
	l.OutputColumns = outputColumns

	// auto populate id and ingest timestamp
	l.OutputColumns[constants.TpID] = xid.New().String()
	l.OutputColumns[constants.TpIngestTimestamp] = time.Now()

	// if no index is set, set the the default
	if tpIndex, ok := l.OutputColumns[constants.TpIndex].(string); !ok || tpIndex == "" {
		l.OutputColumns[constants.TpIndex] = schema.DefaultIndex
	}

	// if we have a tp_timestamp, populate the tp_date
	if tpTimestamp, ok := l.OutputColumns[constants.TpTimestamp].(time.Time); ok && !tpTimestamp.IsZero() {
		l.OutputColumns[constants.TpDate] = tpTimestamp.Truncate(24 * time.Hour)
	}

	return nil
}

func (l *DynamicRow) Validate() error {
	var missingFields []string
	var invalidFields []string

	// Define time fields that need validation
	timeFields := map[string]bool{
		constants.TpIngestTimestamp: true,
		constants.TpTimestamp:       true,
		constants.TpDate:            true,
	}

	// Define required string fields
	requiredStringFields := map[string]bool{
		constants.TpID:         true,
		constants.TpSourceType: true,
		constants.TpTable:      true,
		constants.TpPartition:  true,
		constants.TpIndex:      true,
	}

	// Validate time fields
	for field := range timeFields {
		if err := l.validateTime(l.OutputColumns[field]); err != nil {
			missingFields = append(missingFields, field)
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
	for field := range requiredStringFields {
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

	var missingFieldsStr, invalidFieldsStr string
	if len(missingFields) > 0 {
		missingFieldsStr = fmt.Sprintf("missing required %s: %s", utils.Pluralize("field", len(missingFields)), strings.Join(missingFields, ", "))
	}
	if len(invalidFields) > 0 {
		invalidFieldsStr = fmt.Sprintf("invalid fields: %s", strings.Join(invalidFields, ", "))
	}

	// Concatenate the messages without extra spaces
	errorMsg := missingFieldsStr
	if missingFieldsStr != "" && invalidFieldsStr != "" {
		errorMsg += " "
	}
	errorMsg += invalidFieldsStr

	if errorMsg != "" {
		return fmt.Errorf("row validation failed: %s", errorMsg)
	}
	return nil
}

var missingFieldError = "missing field"

func (l *DynamicRow) validateTime(t interface{}) error {
	if t == nil {
		return errors.New(missingFieldError)
	}

	timeValue, ok := t.(time.Time)
	if !ok || timeValue.IsZero() {
		return errors.New(missingFieldError)
	}

	return nil
}

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.OutputColumns)
}

func (l *DynamicRow) GetSourceValue(s string) (string, bool) {
	v, ok := l.sourceColumns[s]
	return v, ok
}
