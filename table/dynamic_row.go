package table

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

func NewDynamicRow() *DynamicRow {
	return &DynamicRow{
		Columns: make(map[string]string),
	}
}

// InitialiseFromMap initializes the struct from a map of string values
func (l *DynamicRow) InitialiseFromMap(m map[string]string) error {
	l.Columns = m
	return nil
}

// Enrich uses the provided mappings to populate the common fields from mapped column values
func (l *DynamicRow) Enrich(sourceCommonFields schema.CommonFields) error {
	// we expect the columns to be initialised by a previous call to InitialiseFromMap but if not, create it
	if l.Columns == nil {
		l.Columns = make(map[string]string)
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
	// convert the common fields to a map and overlay the dynamic columns
	// we do this to ensure values are correctly formatted
	return json.Marshal(l.Columns)
}

// ResolveSchema returns the (potentially partial) schema for the dynamic row
// - this will be used for the JSONL-parquet conversion
func (l *DynamicRow) ResolveSchema(customTableSchema *schema.TableSchema) (*schema.TableSchema, error) {
	if customTableSchema == nil {
		return nil, fmt.Errorf("no schema provided for dynamic row")
	}
	// get the schema from the common fields
	s, err := schema.SchemaFromStruct(schema.CommonFields{})
	if err != nil {
		return nil, err
	}

	for _, c := range customTableSchema.Columns {
		// skip the common fields
		if schema.IsCommonField(c.ColumnName) {
			continue
		}
		s.Columns = append(s.Columns, &schema.ColumnSchema{
			ColumnName: c.ColumnName,
			// NOTE: do not set the source from the table schema - just use the column name
			// - the source in the table config relates to the mapping from raw row to mapped rown
			// this schema will be used to convert the JSONL (i.e. the mapped row) to parquet
			SourceName: c.ColumnName,
			Type:       c.Type,
			Required:   c.Required,
		})
	}

	s.AutoMapSourceFields = customTableSchema.AutoMapSourceFields
	return s, nil
}
