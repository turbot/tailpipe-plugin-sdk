package helpers

import (
	"fmt"
	"github.com/turbot/pipe-fittings/filter"
)

// BuildFilterMap parses the provided filter strings and returns a map of field name to SQL filters.
// Note: this will fail if any filter refers to more than one field
func BuildFilterMap(filterString []string) (map[string]*filter.SqlFilter, error) {
	filters := make(map[string]*filter.SqlFilter)
	for _, filterString := range filterString {
		// Create a new SQL filter
		f, err := filter.NewSqlFilter(filterString)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filter %s: %v", filterString, err)
		}

		// Extract field names from the filter - this gives a list of all LHS property names (<fieldName> = )
		// Each filter must only have a single field name
		fieldNames, err := f.GetFieldNames()
		if err != nil {
			return nil, fmt.Errorf("failed to get field names for filter %s: %v", filterString, err)
		}

		// Ensure the filter applies to exactly one field
		if len(fieldNames) != 1 {
			return nil, fmt.Errorf("expected a single field name for filter %s, got %v", filterString, fieldNames)
		}

		// Map the filter to its field name
		filters[fieldNames[0]] = f
	}
	return filters, nil
}
