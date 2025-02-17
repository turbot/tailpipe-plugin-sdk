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
	types.CustomTableDef
	Format parse.Config
}

func (c *CustomTableImpl) GetMapper() (mappers.Mapper[*DynamicRow], error) {

	var mapper mappers.CustomTableMapper[*DynamicRow]
	var err error

	switch t := any(c.Format).(type) {
	case *formats.Grok:
		mapper, err = mappers.NewGrokMapper[*DynamicRow](t.Layout, t.Patterns)
	//case *formats.Regex:
	//	return mappers.NewRegexMapper[*DynamicRow](t.Layout, t.Patterns)
	//
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
	err := row.Enrich(sourceEnrichmentFields.CommonFields)
	if err != nil {
		return nil, err
	}
	return row, nil
}
