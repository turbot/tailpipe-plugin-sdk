package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"

	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DynamicRow struct {
	// the source columns as a string map (the format output by the mappers)
	SourceColumns map[string]string

	// the output columns, as a map of string to interface{} - the result of enrichment and type conversion
	OutputColumns map[string]interface{}
}

func (l *DynamicRow) InitialiseFromMap(m map[string]string) error {
	// just assign the source columns
	l.SourceColumns = m
	l.OutputColumns = make(map[string]interface{})
	return nil
}

// Enrich uses the provided mappings to populate the common fields from mapped column values
func (l *DynamicRow) Enrich(tableSchema *schema.TableSchema, sourceEnrichmentFields schema.SourceEnrichment) error {
	// we expect the columns to be initialised by a previous call to InitialiseFromMap
	if l.SourceColumns == nil {
		return fmt.Errorf("the DynamicRow struct has not been initialised with a map of columns")
	}

	// merge in source common fields
	for k, v := range sourceEnrichmentFields.CommonFields.AsMap() {
		if _, ok := l.SourceColumns[k]; !ok {
			l.SourceColumns[k] = v
		}
	}

	const timeFormat = time.RFC3339

	// auto populate id and ingest timestamp
	l.SourceColumns[constants.TpID] = xid.New().String()
	l.SourceColumns[constants.TpIngestTimestamp] = time.Now().Format(timeFormat)

	// if no index is set, set the the default
	if l.SourceColumns[constants.TpIndex] == "" {
		l.SourceColumns[constants.TpIndex] = schema.DefaultIndex
	}

	// if we have a tp_timestamp, parse it and update the field
	if timestampStr, ok := l.SourceColumns[constants.TpTimestamp]; ok {
		timestamp, err := helpers.ParseTime(timestampStr)
		if err != nil {
			return fmt.Errorf("error parsing tp_timestamp: %w", err)
		}

		l.SourceColumns[constants.TpTimestamp] = timestamp.Format(timeFormat)
		// also set the date
		l.SourceColumns[constants.TpDate] = timestamp.Truncate(24 * time.Hour).Format(timeFormat)
	}

	// now ask the schema to map the row for uas
	outputColumns, err := tableSchema.MapRow(l.SourceColumns)
	if err != nil {
		return fmt.Errorf("error mapping row: %w", err)
	}
	// set the output columns
	l.OutputColumns = outputColumns

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
			if err.Error() == missingFieldError {
				missingFields = append(missingFields, field)
			} else {
				invalidFields = append(invalidFields, field)
			}
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

var invalidFieldError = "invalid field"
var missingFieldError = "missing field"

func (l *DynamicRow) validateTime(t interface{}) error {
	if t == nil {
		return errors.New(missingFieldError)
	}

	// check if the field is a string
	tStr, ok := t.(string)
	if !ok {
		return errors.New(invalidFieldError)
	}
	if tStr == "" {
		return errors.New(missingFieldError)
	}
	// try to parse the time
	ingestTimestamp, err := time.Parse(time.RFC3339, tStr)
	if err != nil {
		return errors.New(invalidFieldError)
	}

	if ingestTimestamp.IsZero() {
		return errors.New(missingFieldError)
	}

	return nil
}

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.OutputColumns)
}
