package formats

import "fmt"

type SupportedFormats struct {
	Formats         map[string]func() Format
	FormatInstances map[string]Format
	DefaultFormat   string
}

func (f SupportedFormats) GetDefaultFormat() (Format, error) {
	if f.DefaultFormat == "" {
		return nil, nil
	}
	if f.FormatInstances[f.DefaultFormat] == nil {
		return nil, fmt.Errorf("default format not found: %s", f.DefaultFormat)
	}
	return f.FormatInstances[f.DefaultFormat], nil
}
