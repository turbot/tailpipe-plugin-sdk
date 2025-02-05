package schema

import (
	"fmt"
	"regexp"
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
			pt.Type = value
		default:
			return nil, fmt.Errorf("invalid parquet tag: %s, key '%s' not recognized", tag, key)
		}
	}

	// validate the ParquetTag
	return pt.validate()
}

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

var reservedKeywords = map[string]struct{}{
	"ALL":               {},
	"ANALYZE":           {},
	"AND":               {},
	"AS":                {},
	"ASC":               {},
	"CASE":              {},
	"CAST":              {},
	"CHECK":             {},
	"COLLATE":           {},
	"COLUMN":            {},
	"CONSTRAINT":        {},
	"CREATE":            {},
	"CROSS":             {},
	"CURRENT_DATE":      {},
	"CURRENT_TIME":      {},
	"CURRENT_TIMESTAMP": {},
	"DEFAULT":           {},
	"DEFERRABLE":        {},
	"DESC":              {},
	"DISTINCT":          {},
	"DO":                {},
	"ELSE":              {},
	"END":               {},
	"EXCEPT":            {},
	"FOR":               {},
	"FOREIGN":           {},
	"FROM":              {},
	"FULL":              {},
	"GLOB":              {},
	"GROUP":             {},
	"HAVING":            {},
	"IN":                {},
	"INITIALLY":         {},
	"INNER":             {},
	"INTERSECT":         {},
	"INTO":              {},
	"IS":                {},
	"ISNULL":            {},
	"JOIN":              {},
	"LEFT":              {},
	"LIKE":              {},
	"LIMIT":             {},
	"NATURAL":           {},
	"NOT":               {},
	"NOTNULL":           {},
	"NULL":              {},
	"OFFSET":            {},
	"ON":                {},
	"OR":                {},
	"ORDER":             {},
	"OUTER":             {},
	"PRIMARY":           {},
	"REFERENCES":        {},
	"RIGHT":             {},
	"SELECT":            {},
	"TABLE":             {},
	"THEN":              {},
	"TO":                {},
	"UNION":             {},
	"UNIQUE":            {},
	"USING":             {},
	"WHEN":              {},
	"WHERE":             {},
	"WINDOW":            {},
	"WITH":              {},
}

func (t *ParquetTag) validate() (*ParquetTag, error) {
	quotedRegex := regexp.MustCompile(`^".*"$`)
	unquotedRegex := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

	// Quoted identifiers can use any keyword, whitespace or special character, e.g., "SELECT" and " § 🦆 ¶ " are valid identifiers. https://duckdb.org/docs/sql/dialect/keywords_and_identifiers.html#identifiers
	if !quotedRegex.MatchString(t.Name) {
		// Unquoted identifiers must start with a letter and can only contain letters, numbers, and underscores
		if !unquotedRegex.MatchString(t.Name) {
			return nil, fmt.Errorf("invalid parquet tag: 'name' must be a valid DuckDB identifier")
		}
		// Check if the name is a reserved keyword
		if _, reserved := reservedKeywords[strings.ToUpper(t.Name)]; reserved {
			return nil, fmt.Errorf("invalid parquet tag: 'name' cannot be a reserved keyword")
		}
	}

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
