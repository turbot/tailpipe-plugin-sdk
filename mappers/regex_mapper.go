package mappers

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/turbot/pipe-fittings/v2/utils"
)

type RegexMapper[T MapInitialisedRow] struct {
	re      *regexp.Regexp
	pattern string
}

// NewRegexMapper creates a new RegexMapper with the provided pattern.
func NewRegexMapper[T MapInitialisedRow](pattern string) (*RegexMapper[T], error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("error compiling regex pattern: %w", err)
	}
	return &RegexMapper[T]{
		re:      re,
		pattern: pattern,
	}, nil
}

func (c *RegexMapper[T]) Identifier() string {
	return "row_regex_mapper"
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
		return empty, fmt.Errorf("error parsing log line: didn't match regex pattern %s", c.re.String())
	}

	rowMap := make(map[string]string)
	for i, name := range c.re.SubexpNames() {
		if i != 0 && name != "" { // Skip index 0, which is the full match
			rowMap[name] = match[i]
		}
	}

	// Map parsed fields to the row struct
	row := utils.InstanceOf[T]()
	if err = row.InitialiseFromMap(rowMap); err != nil {
		return empty, fmt.Errorf("error initialising row from map: %w", err)
	}
	if len(rowMap) == 0 {
		slog.Warn("grok mapper - no matches found", "layout", c.pattern, "input", input)
	}

	return row, nil
}
