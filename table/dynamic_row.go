package table

import (
	"encoding/json"
	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"time"
)

type DynamicRow struct {
	// NOTE: no JSON tags are required here as we override the JSON serialization
	enrichment.CommonFields

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
func (l *DynamicRow) Enrich(mappings enrichment.CommonFieldsMappings, fields enrichment.CommonFields) {
	l.CommonFields = fields

	// Standard record enrichment
	l.TpID = xid.New().String()
	l.TpIngestTimestamp = time.Now()

	// init common fields using the mappings and our column values
	l.CommonFields.InitialiseFromMap(l.Columns, mappings)

	// if no index is set, set the the default
	if l.TpIndex == "" {
		l.TpIndex = enrichment.DefaultIndex
	}
	// if tpDate is not set, and tpTimestamp is, set tpDate to the date part of tpTimestamp
	if l.TpDate.IsZero() && !l.TpTimestamp.IsZero() {
		l.TpDate = l.TpTimestamp.Truncate(24 * time.Hour)
	}
}

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	res := l.CommonFields.AsMap()

	for k, v := range l.Columns {
		res[k] = v
	}
	return json.Marshal(res)
}
