package table

import (
	"context"
	"fmt"
	"github.com/satyrius/gonx"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type DelimitedLineMapper[T MapInitialisedModel] struct {
	parsers []*gonx.Parser
	newRow  func() T
}

func NewDelimitedLineMapper[T MapInitialisedModel](newRow func() T, formats ...string) *DelimitedLineMapper[T] {
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

func (c *DelimitedLineMapper[T]) Map(ctx context.Context, a any) ([]T, error) {
	var out []*types.RowData
	var parsed *gonx.Entry
	var err error
	rowData, ok := a.(*types.RowData)
	if !ok {
		return nil, fmt.Errorf("expected *types.RowData, got %T", a)
	}

	// validate input type is string
	input, ok := a.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", rowData.Data)
	}

	for _, parser := range c.parsers {
		parsed, err = parser.ParseString(input)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("error parsing log line - all formats failed: %w", err)
	}

	row := c.newRow()
	if err := row.InitialiseFromMap(parsed.Fields()); err != nil {
		return nil, fmt.Errorf("error initialising row from map: %w", err)
	}

	return []T{row}, nil
}
