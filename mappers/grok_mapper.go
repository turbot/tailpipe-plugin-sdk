package mappers

//
//type GrokMapper[T table.MapInitialisedRow] struct {
//	grok    *grok.Grok
//	patterns []string
//}
//
//func NewGrokMapper[T table.MapInitialisedRow](patterns ...string) *GrokMapper[T] {
//	return &GrokMapper[T]{
//		grok:     grok.New(),
//		patterns: patterns,
//	}
//}
//
//func (c *GrokMapper[T]) Identifier() string {
//	return "row_grok_mapper"
//}
//
//func (c *GrokMapper[T]) Map(_ context.Context, a any, _ ...table.MapOption[T]) (T, error) {
//	var empty T
//
//	// Validate input type is string
//	input, ok := a.(string)
//	if !ok {
//		return empty, fmt.Errorf("expected string, got %T", a)
//	}
//
//	// Try parsing the log line with each pattern
//	var result map[string]string
//	var err error
//	for _, pattern := range c.patterns {
//		result, err = c.grok.Parse(pattern, input)
//		if err == nil {
//			break
//		}
//	}
//	if err != nil {
//		return empty, fmt.Errorf("error parsing log line - all patterns failed: %w", err)
//	}
//
//	// Map parsed fields to the row struct
//	row := utils.InstanceOf[T]()
//	if err := row.InitialiseFromMap(result); err != nil {
//		return empty, fmt.Errorf("error initializing row from map: %w", err)
//	}
//
//	return row, nil
//}
