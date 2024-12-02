package table

import (
	"context"
	"fmt"
	"sync"

	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type ArtifactDynamicTableImpl[R types.RowStruct, S parse.DynamicTableConfig, T ArtifactDynamicTable[R, S]] struct {
	config      S
	tableSchema *schema.RowSchema
	schemaWg    sync.WaitGroup
}

// Init initialised the dynamic table - try to determine the schema from the config and if none is specified,
// subscribe to the source to determine the schema from the first artifact
func (c *ArtifactDynamicTableImpl[R, S, T]) Init(source row_source.RowSource, config S) error {
	// store the config (needed by Notify)
	c.config = config
	c.schemaWg.Add(1)

	// is the schema specified in the config
	if tableSchema := config.GetSchema(); tableSchema != nil {
		c.setSchema(tableSchema)
		// no need to subscribe to the source if we have a schema
		return nil
	}

	// se we need to determine the schema from the files
	// subscribe to the source
	return source.AddObserver(c)
}

// Notify is called when an event is received from the source
func (c *ArtifactDynamicTableImpl[R, S, T]) Notify(_ context.Context, event events.Event) error {
	switch e := event.(type) {
	case *events.ArtifactDownloaded:
		if c.tableSchema == nil {
			// create an instance of the table and ask ity to determine the schema
			t := utils.InstanceOf[T]()
			s, err := t.DetermineSchemaFromArtifact(e.Info.Name, c.config)
			if err != nil {
				return err
			}
			c.setSchema(s)
		}
	}
	return nil
}

// GetSchema waits for the schema to be available and returns the schema of the table
func (c *ArtifactDynamicTableImpl[R, S, T]) GetSchema() (*schema.RowSchema, error) {
	// wait for schemas to be available
	c.schemaWg.Wait()
	if c.tableSchema == nil {
		// unexpected
		return nil, fmt.Errorf("schema not available")
	}
	return c.tableSchema, nil
}

// GetColumns waits for the schema to be available and returns the columns of the table
func (c *ArtifactDynamicTableImpl[R, S, T]) GetColumns() []string {
	// wait for schema to be available
	c.schemaWg.Wait()

	var columns []string
	for _, row := range c.tableSchema.Columns {
		columns = append(columns, row.ColumnName)
	}
	return columns
}

func (c *ArtifactDynamicTableImpl[R, S, T]) setSchema(tableSchema *schema.RowSchema) {
	c.tableSchema = tableSchema
	c.schemaWg.Done()
}
