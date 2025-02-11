package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// CustomTableImpl is a generic struct representing a plugin table definition with a format
type CustomTableImpl struct {
	Format *formats.Custom
}

func (c *CustomTableImpl) SetFormat(format *formats.Custom) {
	c.Format = format
}

func (c *CustomTableImpl) GetMapper() (mappers.Mapper[*DynamicRow], error) {
	// c.Format will already be populated by our CustomTableImpl
	return mappers.NewGrokMapper[*DynamicRow](c.Format.Layout, c.Format.Patterns)
}

func (c *CustomTableImpl) EnrichRow(row *DynamicRow, sourceEnrichmentFields schema.SourceEnrichment) (*DynamicRow, error) {
	// tell the row to enrich itself using any mappings specified in the source format
	err := row.Enrich(sourceEnrichmentFields.CommonFields)
	if err != nil {
		return nil, err
	}
	return row, nil
}
