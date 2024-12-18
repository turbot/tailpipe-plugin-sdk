package table

import (
	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"time"
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
func (l *DynamicRow) Enrich(fields enrichment.CommonFields) {
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
	if _, indexSet := l.Columns["tp_index"]; !indexSet {
		l.Columns["tp_index"] = enrichment.DefaultIndex
	}
	// if tp_date is not set, and tp_timestamp is, set tpDate to the date part of tpTimestamp
	_, dateSet := l.Columns["tp_date"]
	timestamp, timestampSet := l.Columns["tp_timestamp"]
	if !dateSet && timestampSet {
		if t, err := time.Parse(timeFormat, timestamp); err == nil {
			l.Columns["tp_date"] = t.Truncate(24 * time.Hour).Format(timeFormat)
		}
	}
}

func (l *DynamicRow) Validate() error {
	commonFields := l.GetCommonFields()
	return commonFields.Validate()
}

func (l *DynamicRow) GetCommonFields() enrichment.CommonFields {
	var res enrichment.CommonFields
	res.InitialiseFromMap(l.Columns, &schema.RowSchema{})
	return res
}
