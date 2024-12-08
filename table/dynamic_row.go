package table

import (
	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"time"
)

type DynamicRow struct {
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
func (l *DynamicRow) Enrich(mappings *enrichment.CommonFieldsMappings, fields enrichment.CommonFields) {
	l.CommonFields = fields

	// Standard record enrichment
	l.TpID = xid.New().String()
	l.TpIngestTimestamp = time.Now()

	// init common fields using the mappings and our column values
	l.CommonFields.InitFromMap(l.Columns, mappings)

	// if no index is set, set the the default
	if l.TpIndex == "" {
		l.TpIndex = enrichment.DefaultIndex
	}
}
