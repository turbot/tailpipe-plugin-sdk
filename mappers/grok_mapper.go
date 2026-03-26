package mappers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"regexp/syntax"
	"unsafe"

	"github.com/elastic/go-grok"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
)

type GrokMapper[T MapInitialisedRow] struct {
	parser *grok.Grok
	layout string
}

// NewGrokMapper creates a new GrokMapper which contains a grok parser for each layout.
// patterns is a map of pattern names to grok patterns.
func NewGrokMapper[T MapInitialisedRow](layout string, patterns map[string]string) (*GrokMapper[T], error) {
	res := &GrokMapper[T]{
		layout: layout,
	}

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

	// Map parsed fields to the row struct
	row := utils.InstanceOf[T]()
	if err := row.InitialiseFromMap(rowMap); err != nil {
		return empty, fmt.Errorf("error initializing row from map: %w", err)
	}
	// for pattern debugging purposes, if there were no matches, we want to log the input and the result
	if len(result) == 0 {
		slog.Warn("grok mapper - no matches found", "layout", c.layout, "input", input)
	}
	return row, nil
}

func (c *GrokMapper[T]) GetRegex() (_ string, err error) {
	if c.parser == nil {
		return "", fmt.Errorf("no parser configured")
	}
	// unfortunately the regex field in the grok parser is private so we need to use reflection to get it
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to get regex from grok parser: %w", helpers.ToError(r))
		}
	}()
	parserValue := reflect.ValueOf(c.parser).Elem()
	regexField := parserValue.FieldByName("re")

	// Ensure the field is valid and addressable
	if !regexField.IsValid() || !regexField.CanAddr() {
		return "", fmt.Errorf("could not find or access regex field in grok.Grok")
	}

	// Ensure the field is of the correct type
	if regexField.Type() != reflect.TypeOf(&regexp.Regexp{}) {
		return "", fmt.Errorf("regex field is not of type *regexp.Regexp")
	}

	// Get the value of the regex field
	regexPtr := (*regexp.Regexp)(unsafe.Pointer(regexField.UnsafeAddr()))
	return regexPtr.String(), nil
}
