package table

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// CustomTableImpl is a generic struct representing a plugin table definition with a format
type CustomTableImpl struct {
	Schema *schema.TableSchema
	// the format
	Format formats.Format
}

// Initialize sets the format and schema for the table
func (c *CustomTableImpl) Initialize(format formats.Format, customTableSchema *schema.TableSchema) {
	c.Format = format
	// merge the custom table schema with the common fields schema
	c.Schema = customTableSchema.MergeWithCommonSchema()
}

func (c *CustomTableImpl) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	mapper, err := c.Format.GetMapper()

	// TODO KAI stop passing schema to mapper
	// all mappers returned by this function should support SetSchema
	type SchemaSetter interface {
		SetSchema(*schema.TableSchema)
	}
	ss, ok := mapper.(SchemaSetter)
	if !ok {
		return nil, fmt.Errorf("mapper %T does not support SetSchema", mapper)
	}
	ss.SetSchema(c.Schema)

	return mapper, err
}

// GetSchema implements the CustomTable interface
func (c *CustomTableImpl) GetSchema() *schema.TableSchema {
	return c.Schema
}

func (c *CustomTableImpl) EnrichRow(row *types.DynamicRow, sourceEnrichmentFields schema.SourceEnrichment) (*types.DynamicRow, error) {
	// tell the row to enrich itself using any mappings specified in the source format
	err := row.Enrich(sourceEnrichmentFields.CommonFields)
	if err != nil {
		return nil, err
	}
	return row, nil
}
