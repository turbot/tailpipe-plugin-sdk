package schema

import (
	"regexp"
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

// IsValidColumnType checks if a column type is valid in DuckDB.
func IsValidColumnType(columnType string) bool {
	_, isValid := validDuckDBTypes[columnType]
	return isValid
}
