package schema

import (
	"fmt"
	"strings"

	"golang.org/x/exp/maps"
)

// ParquetTag represents the components of a parquet tag
type ParquetTag struct {
	Name string
	Type string
	Skip bool
}

// ParseParquetTag parses and validates a parquet tag string
func ParseParquetTag(tag string) (*ParquetTag, error) {

	// Initialize the ParquetTag with the type
	pt := &ParquetTag{}

	// NOTE: if tag is "-" then skip the field
	if tag == "-" {
		pt.Skip = true
		return pt, nil
	}

	// Split the tag into components
	parts := strings.Split(tag, ",")
	for _, part := range parts {
		// trim spaces
		part = strings.TrimSpace(part)

		// split on '='
		kv := strings.Split(part, "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid parquet tag: %s - one of 'name' and 'type' must be set ", tag)
		}

		// trim spaces in key and value
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		switch key {
		case "name":
			pt.Name = value
		case "type":
			pt.Type = strings.ToLower(value)
		default:
			return nil, fmt.Errorf("invalid parquet tag: %s, key '%s' not recognized", tag, key)
		}
	}

	// validate the ParquetTag
	return pt.validate()
}

func (t *ParquetTag) validate() (*ParquetTag, error) {
	if t.Name != "" && !IsValidColumnName(t.Name) {
		return nil, fmt.Errorf("invalid parquet tag: 'name' must be a valid DuckDB column name")
	}

	if t.Type != "" && !IsValidColumnType(t.Type) {
		return nil, fmt.Errorf("invalid parquet tag: 'type' must be one of %v", maps.Keys(validDuckDBTypes))
	}
	// If everything is valid, return the ParquetTag instance
	return t, nil
}
