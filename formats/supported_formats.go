package formats

import "fmt"

type SupportedFormats struct {
	Formats         map[string]func() Format
	FormatInstances []Format
	DefaultFormat   string
}

func (f SupportedFormats) GetDefaultFormat() (Format, error) {
	if f.DefaultFormat == "" {
		return nil, nil
	}
	for _, format := range f.FormatInstances {
		if format.Identifier() == f.DefaultFormat {
			return format, nil
		}
	}
	return nil, fmt.Errorf("default format not found: %s", f.DefaultFormat)
}
