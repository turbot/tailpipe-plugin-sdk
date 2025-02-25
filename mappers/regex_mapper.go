package mappers

import (
	"context"
	"fmt"
	"regexp"

	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// TODO REMOVE SCHEMA
type RegexMapper[T MapInitialisedRow] struct {
	re *regexp.Regexp
	// TODO REMOVE SCHEMA
	schema *schema.TableSchema
}

// NewRegexMapper creates a new RegexMapper with the provided pattern.
func NewRegexMapper[T MapInitialisedRow](pattern string) (*RegexMapper[T], error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("error compiling regex pattern: %w", err)
	}
	return &RegexMapper[T]{re: re}, nil
}

func (c *RegexMapper[T]) Identifier() string {
	return "row_regex_mapper"
}

// TODO REMOVE SCHEMA
// SetSchema implements SchemaSetter interface
func (c *RegexMapper[T]) SetSchema(schema *schema.TableSchema) {
	c.schema = schema
}

func (c *RegexMapper[T]) Map(_ context.Context, a any, opts ...MapOption[T]) (T, error) {
	var empty T
	var err error

	// apply opts - this may set a schema
	for _, opt := range opts {
		opt(c)
	}

	// Validate input type is string
	input, ok := a.(string)
	if !ok {
		return empty, fmt.Errorf("expected string, got %T", a)
	}

	// Parse the input string
	match := c.re.FindStringSubmatch(input)
	if match == nil {
		return empty, fmt.Errorf("error parsing log line, didn't match regex pattern %s", c.re.String())
	}

	rowMap := make(map[string]string)
	for i, name := range c.re.SubexpNames() {
		if i != 0 && name != "" { // Skip index 0, which is the full match
			rowMap[name] = match[i]
		}
	}

	// if we have a schema, apply the schema to map any required

	// Map parsed fields to the row struct
	row := utils.InstanceOf[T]()
	// TODO REMOVE SCHEMA
	if err = row.InitialiseFromMap(rowMap, c.schema); err != nil {
		return empty, fmt.Errorf("error initialising row from map: %w", err)
	}

	return row, nil
}
