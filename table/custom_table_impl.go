package table

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// CustomTableImpl is a generic struct representing a plugin table definition with a format
type CustomTableImpl struct {
	Name   string
	Schema *schema.RowSchema
	Format parse.Config
}

// Initialize sets the format and schema for the table
func (c *CustomTableImpl) Initialize(format parse.Config, tableDef *types.CustomTableDef) {
	c.Format = format
	c.Name = tableDef.Name
	c.Schema = tableDef.Schema
}

func (c *CustomTableImpl) GetMapper() (mappers.Mapper[*DynamicRow], error) {
	var mapper mappers.CustomTableMapper[*DynamicRow]
	var err error

	switch t := any(c.Format).(type) {
	case *formats.Grok:
		mapper, err = mappers.NewGrokMapper[*DynamicRow](t.Layout, t.Patterns)
	case *formats.Regex:
		mapper, err = mappers.NewRegexMapper[*DynamicRow](t.Layout)

	default:
		return nil, fmt.Errorf("unsupported format type: %T", t)
	}

	// all mappers returned by this function should support SetSchema
	type SchemaSetter interface {
		SetSchema(*schema.RowSchema)
	}
	ss, ok := mapper.(SchemaSetter)
	if !ok {
		return nil, fmt.Errorf("mapper %T does not support SetSchema", mapper)
	}
	ss.SetSchema(c.Schema)

	return mapper, err
}

func (c *CustomTableImpl) EnrichRow(row *DynamicRow, sourceEnrichmentFields schema.SourceEnrichment) (*DynamicRow, error) {
	// tell the row to enrich itself using any mappings specified in the source format
	err := row.Enrich(c.Schema, sourceEnrichmentFields.CommonFields)
	if err != nil {
		return nil, err
	}
	return row, nil
}
