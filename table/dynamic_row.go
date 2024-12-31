package table

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
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
func (l *DynamicRow) Enrich(fields schema.CommonFields) {
	for k, v := range fields.AsMap() {
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

	// if tp_date is not set, and tp_timestamp is, set tpDate to the date part of tpTimestamp
	// we know the fitled WILL be there as it isa value type but is it zero?
	// is date zero
	var zeroDate time.Time
	dateSet := l.Columns["tp_date"] == zeroDate.String()

	timestamp := l.Columns["tp_timestamp"]
	if !dateSet && timestamp != zeroDate.String() {
		if t, err := time.Parse(timeFormat, timestamp); err == nil {
			l.Columns["tp_date"] = t.Truncate(24 * time.Hour).Format(timeFormat)
		}
	}
}

func (l *DynamicRow) Validate() error {
	commonFields := l.GetCommonFields()
	return commonFields.Validate()
}

func (l *DynamicRow) GetCommonFields() schema.CommonFields {
	var res schema.CommonFields
	res.InitialiseFromMap(l.Columns)
	return res
}

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.Columns)
}

// ResolveSchema returns the (potentially partial) schema for the dynamic row
// - this will be used for the JSONL-parquet conversion
func (l *DynamicRow) ResolveSchema(customTable *types.Table) (*schema.RowSchema, error) {
	if customTable.Schema == nil {
		return nil, fmt.Errorf("no schema provided for dynamic row")
	}
	// get the schema from the common fields
	s, err := schema.SchemaFromStruct(schema.CommonFields{})
	if err != nil {
		return nil, err
	}

	for _, c := range customTable.Schema.Columns {
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
		})
	}

	s.AutoMapSourceFields = customTable.Schema.AutoMapSourceFields
	return s, nil
}
