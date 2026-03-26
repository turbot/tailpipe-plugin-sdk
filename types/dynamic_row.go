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

// MarshalJSON overrides JSON serialization to include the dynamic columns
func (l *DynamicRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.OutputColumns)
}

func (l *DynamicRow) GetSourceValue(s string) (string, bool) {
	v, ok := l.sourceColumns[s]
	return v, ok
}
