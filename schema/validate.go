package schema

import (
	"regexp"
	"strings"
)

// IsValidColumnName checks if a column name is valid in DuckDB.
func IsValidColumnName(name string) bool {
	// Check for empty name
	if len(name) == 0 {
		return false
	}
	// DuckDB column names must start with a letter or underscore and contain only letters, digits, or underscores.
	validNameRegex := `^[a-zA-Z_][a-zA-Z0-9_]*$`
	matched, err := regexp.MatchString(validNameRegex, name)
	if err != nil || !matched {
		return false
	}
	// Ensure the name isn't too long
	return len(name) <= 255
}

// Lookup table of valid DuckDB types
// Note: this excludes structs, lists, and other complex types
var validDuckDBTypes = map[string]struct{}{
	// TODO #schema test all types for parquet conversion https://github.com/turbot/tailpipe-plugin-sdk/issues/22
	"boolean":   {},
	"tinyint":   {},
	"smallint":  {},
	"integer":   {},
	"bigint":    {},
	"utinyint":  {},
	"usmallint": {},
	"uinteger":  {},
	"ubigint":   {},
	"float":     {},
	"double":    {},
	"varchar":   {},
	"blob":      {},
	"date":      {},
	"timestamp": {},
	"time":      {},
	"interval":  {},
	"decimal":   {},
	"uuid":      {},
	"json":      {},
	"struct":    {},
}

func IsValidColumnType(ty string) bool {
	// Convert type to lower case for case-insensitive comparison
	normalizedType := strings.ToLower(ty)

	// special case - we do not support struct arrays for column types
	if ty == "struct[]" {
		return false
	}

	// strip trailing `[]` - just check the underlying type
	normalizedType = strings.TrimSuffix(normalizedType, "[]")
	_, valid := validDuckDBTypes[normalizedType]
	return valid
}
