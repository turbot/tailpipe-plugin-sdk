package table

import (
	"context"
	"fmt"

	"github.com/satyrius/gonx"
)

type DelimitedLineMapper[T MapInitialisedRow] struct {
	parsers []*gonx.Parser
	newRow  func() T
}

func NewDelimitedLineMapper[T MapInitialisedRow](newRow func() T, formats ...string) *DelimitedLineMapper[T] {
	res := &DelimitedLineMapper[T]{
		newRow: newRow,
	}
	for _, format := range formats {
		res.parsers = append(res.parsers, gonx.NewParser(format))
	}
	return res
}

func (c *DelimitedLineMapper[T]) Identifier() string {
	return "delimited_line_logger"
}

func (c *DelimitedLineMapper[T]) Map(ctx context.Context, a any) (T, error) {
	var parsed *gonx.Entry
	var err error
	var empty T

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

	row := c.newRow()
	if err := row.InitialiseFromMap(parsed.Fields()); err != nil {
		return empty, fmt.Errorf("error initialising row from map: %w", err)
	}

	return row, nil
}
