package mappers

import (
	"context"
	"errors"
	"fmt"
	"regexp/syntax"

	"github.com/elastic/go-grok"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type GrokMapper[T MapInitialisedRow] struct {
	parser *grok.Grok

	schema *schema.RowSchema
}

// NewGrokMapper creates a new GrokMapper which contains a grok parser for each layout.
// patterns is a map of pattern names to grok patterns.
func NewGrokMapper[T MapInitialisedRow](layout string, patterns map[string]string) (*GrokMapper[T], error) {
	res := &GrokMapper[T]{}

	g := grok.New()
	if err := g.AddPatterns(patterns); err != nil {
		return nil, fmt.Errorf("error adding patterns: %w", err)
	}
	if err := g.Compile(layout, true); err != nil {
		//is this a *syntax.Error
		var e = &syntax.Error{}
		if errors.As(err, &e) {
			return nil, fmt.Errorf("syntax error compiling layout: %s", e.Code)
		} else {
			return nil, fmt.Errorf("syntax error compiling layout: %w", err)
		}
	}
	res.parser = g

	return res, nil
}

func (c *GrokMapper[T]) Identifier() string {
	return "grok_mapper"
}

func (c *GrokMapper[T]) SetSchema(schema *schema.RowSchema) {
	c.schema = schema
}
func (c *GrokMapper[T]) Map(_ context.Context, a any, opts ...MapOption[T]) (T, error) {
	var empty T

	for _, opt := range opts {
		opt(c)
	}

	// Validate input type is string
	input, ok := a.(string)
	if !ok {
		return empty, fmt.Errorf("expected string, got %T", a)
	}

	// Parse the input string
	result, err := c.parser.Parse([]byte(input))
	if err != nil {
		return empty, fmt.Errorf("error parsing log line - all patterns failed: %w", err)
	}

	rowMap := helpers.ByteMapToStringMap(result)
	// if we have a schema, apply the schema to map any required
	if c.schema != nil {
		rowMap, err = c.schema.MapRow(rowMap)
		if err != nil {
			return empty, fmt.Errorf("error applying schema: %w", err)
		}
	}

	// Map parsed fields to the row struct
	row := utils.InstanceOf[T]()
	if err := row.InitialiseFromMap(rowMap); err != nil {
		return empty, fmt.Errorf("error initializing row from map: %w", err)
	}

	return row, nil
}
