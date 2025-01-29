package mappers

import (
	"context"
	"fmt"

	"github.com/satyrius/gonx"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

type GonxMapper[T table.MapInitialisedRow] struct {
	parsers []*gonx.Parser
	schema  *schema.RowSchema
}

func NewGonxMapper[T table.MapInitialisedRow](formats ...string) *GonxMapper[T] {
	res := &GonxMapper[T]{}
	for _, format := range formats {
		res.parsers = append(res.parsers, gonx.NewParser(format))
	}
	return res
}

// SetSchema implements SchemaSetter interface
func (c *GonxMapper[T]) SetSchema(schema *schema.RowSchema) {
	c.schema = schema
}

func (c *GonxMapper[T]) Identifier() string {
	return "row_pattern_mapper"
}

func (c *GonxMapper[T]) Map(_ context.Context, a any, opts_ ...table.MapOption[T]) (T, error) {
	// apply opts - this may set a schema
	for _, opt := range opts_ {
		opt(c)
	}

	var parsed *gonx.Entry
	var err error
	var empty T

	// we must have at least one parser
	if len(c.parsers) == 0 {
		return empty, fmt.Errorf("no parsers configured")
	}

	// validate input type is string
	input, ok := a.(string)
	if !ok {
		return empty, fmt.Errorf("expected string, got %T", a)
	}

	for _, parser := range c.parsers {
		parsed, err = parser.ParseString(input)
		if err == nil {
			break
		}
	}
	if err != nil {
		return empty, fmt.Errorf("error parsing log line - all formats failed: %w", err)
	}

	rowMap := parsed.Fields()
	// if we have a schema, apply the schema to map any required
	rowMap, err = c.schema.MapRow(rowMap)
	if err != nil {
		return empty, fmt.Errorf("error applying schema: %w", err)
	}

	row := utils.InstanceOf[T]()
	if err := row.InitialiseFromMap(rowMap); err != nil {
		return empty, fmt.Errorf("error initialising row from map: %w", err)
	}

	return row, nil
}
