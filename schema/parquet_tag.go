package schema

import (
	"fmt"
	"golang.org/x/exp/maps"

	"strings"
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
			pt.Type = value
		default:
			return nil, fmt.Errorf("invalid parquet tag: %s, key '%s' not recognized", tag, key)
		}
	}

	// validate the ParquetTag
	return pt.validate()
}

// TODO K use defs for these, shared with CLI??
// TODO K func Testall of these(t *testing.T)
// Define valid DuckDB types using a struct{} map for efficient membership checking
var validDuckDBTypes = map[string]struct{}{
	// TODO #schema STRUCT/LIST/ https://github.com/turbot/tailpipe-plugin-sdk/issues/21
	// TODO #schema test all types for parquet conversion https://github.com/turbot/tailpipe-plugin-sdk/issues/22

	"BOOLEAN":   {},
	"TINYINT":   {},
	"SMALLINT":  {},
	"INTEGER":   {},
	"BIGINT":    {},
	"UTINYINT":  {},
	"USMALLINT": {},
	"UINTEGER":  {},
	"UBIGINT":   {},
	"FLOAT":     {},
	"DOUBLE":    {},
	"VARCHAR":   {},
	"BLOB":      {},
	"DATE":      {},
	"TIMESTAMP": {},
	"TIME":      {},
	"INTERVAL":  {},
	"DECIMAL":   {},
	"UUID":      {},
	"JSON":      {},
}

func (t *ParquetTag) validate() (*ParquetTag, error) {
	// TODO #validation validate name is duckdb compliant?

	if t.Type != "" {
		// Convert type to upper case for case-insensitive comparison
		normalizedType := strings.ToUpper(t.Type)
		if _, valid := validDuckDBTypes[normalizedType]; !valid {
			return nil, fmt.Errorf("invalid parquet tag: 'type' must be one of %v", maps.Keys(validDuckDBTypes))
		}
	}
	// If everything is valid, return the ParquetTag instance
	return t, nil
}
