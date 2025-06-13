package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// CustomTableImpl is a generic struct representing a plugin table definition with a format
type CustomTableImpl struct {
	// the full schema, i.e. common fields AND any custom schema,
	// defined either in the plugin config or by the predefined custom table
	Schema *schema.TableSchema
	// the custom schema only
	customTableSchema *schema.TableSchema
	// the format
	Format formats.Format
}

// Initialize sets the format and schema for the table
func (c *CustomTableImpl) Initialize(format formats.Format, customTableSchema *schema.TableSchema) error {
	c.Format = format
	c.customTableSchema = customTableSchema
	// merge the custom table schema with the common fields schema
	c.Schema = customTableSchema.MergeWithCommonSchema()

	// ensure the schema types are normalised to the lower case
	c.Schema.NormaliseColumnTypes()
	// validate the schema
	return c.Schema.Validate()
}

// GetSchema implements the CustomTable interface
// and returns the full schema for the table, which includes common fields
func (c *CustomTableImpl) GetSchema() *schema.TableSchema {
	return c.Schema
}

// GetCustomSchema returns custom schema defined in the plugin config or by the predefined custom table
// This DOES NOT include common fields
func (c *CustomTableImpl) GetCustomSchema() *schema.TableSchema {
	return c.customTableSchema
}

func (c *CustomTableImpl) EnrichRow(row *types.DynamicRow, sourceEnrichmentFields schema.SourceEnrichment) (*types.DynamicRow, error) {
	// tell the row to enrich itself using any mappings specified in the source format
	err := row.Enrich(c.Schema, sourceEnrichmentFields)
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (c *CustomTableImpl) GetDefaultFormat() formats.Format {
	return nil
}

func (c *CustomTableImpl) GetFormat() formats.Format {
	return c.Format
}
