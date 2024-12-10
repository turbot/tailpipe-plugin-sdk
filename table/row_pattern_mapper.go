package table

import (
	"context"
	"fmt"
	"github.com/satyrius/gonx"
	"github.com/turbot/pipe-fittings/utils"
)

type RowPatternMapper[T MapInitialisedRow] struct {
	parsers []*gonx.Parser
}

func NewRowPatternMapper[T MapInitialisedRow](formats ...string) *RowPatternMapper[T] {
	res := &RowPatternMapper[T]{}
	for _, format := range formats {
		res.parsers = append(res.parsers, gonx.NewParser(format))
	}
	return res
}

func (c *RowPatternMapper[T]) Identifier() string {
	return "row_pattern_mapper"
}

func (c *RowPatternMapper[T]) Map(ctx context.Context, a any) (T, error) {
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

	row := utils.InstanceOf[T]()
	if err := row.InitialiseFromMap(parsed.Fields()); err != nil {
		return empty, fmt.Errorf("error initialising row from map: %w", err)
	}

	return row, nil
}
