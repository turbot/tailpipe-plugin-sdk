package table

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"strings"
)

type CsvMapper[T MapInitialisedRow] struct {
	columnsFunc func() []string
	// lazy load the columns (may  not be available until the first call to map)
	columns []string
}

func NewCsvMapper[T MapInitialisedRow](columnsFunc func() []string) *CsvMapper[T] {
	return &CsvMapper[T]{
		columnsFunc: columnsFunc,
	}
}

func (c *CsvMapper[T]) Identifier() string {
	return "csv_mapper"
}

func (c *CsvMapper[T]) Map(ctx context.Context, a any) (T, error) {
	var err error
	var empty T

	// TODO are we cerain the schema will be available???
	// lazy load the columns
	if c.columns == nil {
		c.columns = c.columnsFunc()
	}

	// validate input type is string
	input, ok := a.(string)
	if !ok {
		return empty, fmt.Errorf("expected string, got %T", a)
	}

	r := csv.NewReader(strings.NewReader(input))
	r.TrimLeadingSpace = true
	rowValues, err := r.Read()
	if err != nil {
		fmt.Println("Error reading CSV data:", err)
		return empty, err
	}

	// create a map of column name to value
	rowMap := make(map[string]string)

	// verify that the number of columns in the row matches the number of columns in the mapperFunc
	if len(rowValues) != len(c.columns) {
		return empty, fmt.Errorf("expected %d columns, got %d", len(c.columns), len(rowValues))
	}
	for i, column := range c.columns {
		rowMap[column] = rowValues[i]
	}

	// now we have a map of column name to value, we can initialise the row
	row := utils.InstanceOf[T]()
	if err := row.InitialiseFromMap(rowMap); err != nil {
		return empty, fmt.Errorf("error initialising row from map: %w", err)
	}

	return row, nil
}
