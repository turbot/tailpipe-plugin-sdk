package types

import (
	"encoding/json"
	"fmt"
	"golang.org/x/exp/maps"
	"time"

	"github.com/rs/xid"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DynamicRow struct {
	schema.CommonFields
	// dynamic columns
	Columns map[string]string
}

func (l *DynamicRow) InitialiseFromMap(m map[string]string) error {
	l.CommonFields.InitialiseFromMap(m)
	// remove common fields from the map
	for commonField := range schema.DefaultCommonFieldDescriptions {
		delete(m, commonField)
	}
	// now assign remaining fields to columns
	l.Columns = m
	return nil
}

// Enrich uses the provided mappings to populate the common fields from mapped column values
func (l *DynamicRow) Enrich(sourceEnrichmentFields schema.SourceEnrichment) error {
	// we expect the columns to be initialised by a previous call to InitialiseFromMap
	if l.Columns == nil {
		return fmt.Errorf("the DynamicRow struct has not been initialised with a map of columns")
	}
	// merge our common fields with the source enrichment fields
	l.CommonFields.MergeWith(sourceEnrichmentFields.CommonFields)

	// auto populate id and timestamp
	l.TpID = xid.New().String()
	l.TpIngestTimestamp = time.Now()

	// if no index is set, set the the default
	if l.TpIndex == "" {
		l.TpIndex = schema.DefaultIndex
	}

	// if we have a tp_timestamp, parse it and update the field
	if !l.TpTimestamp.IsZero() {
		l.TpDate = l.TpTimestamp.Truncate(24 * time.Hour)
	}

	return nil
}

func (l *DynamicRow) GetCommonFields() schema.CommonFields {
	return l.CommonFields
}

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	// convert common fields to a map
	res := l.CommonFields.AsMap()
	// copy the dynamic columns into the map
	maps.Copy(res, l.Columns)
	// and return the map as JSON
	return json.Marshal(res)
}
