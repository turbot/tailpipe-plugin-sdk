package formats

type SupportedFormats struct {
	Formats map[string]func() Format
	// an optional default format for the table (may be referenced in HCL config)
	DefaultFormat Format
}
