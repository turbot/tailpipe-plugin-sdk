package types

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/xid"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DynamicRow struct {
	// dynamic columns
	Columns map[string]string
}


func (l *DynamicRow) InitialiseFromMap(m map[string]string) error {
	l.Columns = m
	return nil
}

// Enrich uses the provided mappings to populate the common fields from mapped column values
func (l *DynamicRow) Enrich(sourceCommonFields schema.CommonFields) error {

	// we expect the columns to be initialised by a previous call to InitialiseFromMap
	if l.Columns == nil {
		return fmt.Errorf("the DynamicRow struct has not been initialised with a map of columns")
	}

	// apply source common fields
	for k, v := range sourceCommonFields.AsMap() {
		if _, ok := l.Columns[k]; !ok {
			l.Columns[k] = v
		}
	}

	const timeFormat = time.RFC3339

	// auto populate id and timestamp
	l.Columns["tp_id"] = xid.New().String()
	l.Columns["tp_ingest_timestamp"] = time.Now().Format(timeFormat)

	// if no index is set, set the the default
	if l.Columns["tp_index"] == "" {
		l.Columns["tp_index"] = schema.DefaultIndex
	}

	// if we have a tp_timestamp, parse it and update the field
	if timestampStr, ok := l.Columns["tp_timestamp"]; ok {
		timestamp, err := helpers.ParseTime(timestampStr)
		if err != nil {
			return fmt.Errorf("error parsing tp_timestamp: %w", err)
		}

		l.Columns["tp_timestamp"] = timestamp.Format(timeFormat)
		// also set the date
		l.Columns["tp_date"] = timestamp.Truncate(24 * time.Hour).Format(timeFormat)
	}

	return nil
}

func (l *DynamicRow) GetCommonFields() schema.CommonFields {
	return schema.CommonFieldsFromMap(l.Columns)
}

func (l *DynamicRow) Validate() error {
	f := schema.CommonFieldsFromMap(l.Columns)
	return f.Validate()
}

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	// TODO #customtables check this
	// convert the common fields to a map and overlay the dynamic columns
	// we do this to ensure values are correctly formatted
	return json.Marshal(l.Columns)
}
