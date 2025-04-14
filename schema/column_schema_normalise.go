package schema

import "strings"

// Valid DuckDB types and their aliases
var duckDBTypes = map[string]bool{
	// Integer types
	"bigint": true, "int8": true, "long": true,
	"integer": true, "int4": true, "int": true, "signed": true,
	"smallint": true, "int2": true, "short": true,
	"tinyint": true, "int1": true,
	"hugeint": true,

	// Unsigned integer types
	"ubigint":   true,
	"uinteger":  true,
	"usmallint": true,
	"utinyint":  true,
	"uhugeint":  true,

	// Floating point types
	"double": true, "float8": true,
	"float": true, "float4": true, "real": true,

	// Decimal types
	"decimal": true, "numeric": true,

	// String types
	"varchar": true, "char": true, "bpchar": true, "text": true, "string": true,

	// Date/Time types
	"date":      true,
	"time":      true,
	"timestamp": true, "datetime": true,
	"timestamptz": true,

	// Other types
	"boolean": true, "bool": true, "logical": true,
	"blob": true, "bytea": true, "binary": true, "varbinary": true,
	"bit": true, "bitstring": true,
	"uuid":     true,
	"json":     true,
	"interval": true,

	// Nested types
	"array":  true,
	"list":   true,
	"map":    true,
	"struct": true,
	"union":  true,
}

// NormaliseColumnTypes normalizes the type of this column and all its child fields.
// It recursively processes nested structures to ensure all types are in their canonical form.
func (c *ColumnSchema) NormaliseColumnTypes() {
	c.Type = c.normaliseType(c.Type)

	// Normalize struct field types
	for _, field := range c.StructFields {
		field.NormaliseColumnTypes()
	}
}

// normaliseType normalizes a DuckDB type string to its canonical form.
// It handles:
// - Simple types (e.g., "VARCHAR" -> "varchar")
// - Array types (e.g., "VARCHAR[]" -> "varchar[]")
// - Map types (e.g., "MAP<VARCHAR, INTEGER>" -> "map<varchar, integer>")
// - Struct/Union types (e.g., "STRUCT(field VARCHAR)" -> "struct(field varchar)")
// - Nested combinations of the above
func (c *ColumnSchema) normaliseType(typeStr string) string {
	typeStr = normalizeWhitespace(typeStr)

	// Handle array types first to prevent recursion
	if strings.HasSuffix(typeStr, "]") {
		return c.normalizeArrayType(typeStr)
	}

	// Handle map types
	if strings.HasPrefix(strings.ToUpper(typeStr), "MAP") {
		return c.normalizeMapType(typeStr)
	}

	// Handle struct/union types
	if strings.Contains(typeStr, "(") {
		return c.normalizeStructOrUnionType(typeStr)
	}

	// Handle simple types
	if duckDBTypes[strings.ToLower(typeStr)] {
		return strings.ToLower(typeStr)
	}

	return typeStr
}

// normalizeArrayType handles array type normalization
func (c *ColumnSchema) normalizeArrayType(typeStr string) string {
	// Find the last ']' and its matching '['
	lastClose := strings.LastIndex(typeStr, "]")
	if lastClose == -1 {
		return typeStr
	}

	// Find the matching '['
	openCount := 0
	lastOpen := -1
	for i := lastClose; i >= 0; i-- {
		if typeStr[i] == ']' {
			openCount++
		} else if typeStr[i] == '[' {
			openCount--
			if openCount == 0 {
				lastOpen = i
				break
			}
		}
	}

	if lastOpen == -1 {
		return typeStr
	}

	// Extract and normalize the base type
	baseType := strings.TrimSpace(typeStr[:lastOpen])
	normalizedBase := c.normaliseType(baseType)

	// Get the array part and normalize whitespace
	arrayPart := strings.ReplaceAll(strings.ReplaceAll(typeStr[lastOpen:], " ", ""), "\t", "")

	return normalizedBase + arrayPart
}

// normalizeMapType handles the normalization of MAP types.
// It normalizes both the key and value types while preserving the MAP structure.
func (c *ColumnSchema) normalizeMapType(typeStr string) string {
	// Extract the key and value types from the MAP definition
	parts := strings.SplitN(typeStr, "<", 2)
	if len(parts) != 2 {
		return typeStr
	}

	typeName := strings.ToLower(strings.TrimSpace(parts[0]))
	innerTypes := strings.TrimRight(parts[1], ">")

	// Split key and value types
	var keyType, valueType string
	var parenCount, angleCount int
	var splitPos = -1

	// Find the comma that separates key and value types
	for i, char := range innerTypes {
		switch char {
		case '(':
			parenCount++
		case ')':
			parenCount--
		case '<':
			angleCount++
		case '>':
			angleCount--
		case ',':
			if parenCount == 0 && angleCount == 0 {
				splitPos = i
				break
			}
		}
	}

	if splitPos == -1 {
		return typeStr
	}

	keyType = strings.TrimSpace(innerTypes[:splitPos])
	valueType = strings.TrimSpace(innerTypes[splitPos+1:])

	// Normalize both key and value types
	normalizedKey := c.normaliseType(keyType)
	normalizedValue := c.normaliseType(valueType)

	return typeName + "<" + normalizedKey + ", " + normalizedValue + ">"
}

// normalizeStructOrUnionType handles the normalization of STRUCT and UNION types.
// It preserves field names while normalizing their types.
func (c *ColumnSchema) normalizeStructOrUnionType(typeStr string) string {
	// Extract the type name (STRUCT or UNION) and the fields
	parts := strings.SplitN(typeStr, "(", 2)
	if len(parts) != 2 {
		return typeStr
	}

	typeName := strings.ToLower(strings.TrimSpace(parts[0]))
	fieldsStr := strings.TrimRight(parts[1], ")")

	// Handle empty struct/union
	if fieldsStr == "" {
		return typeName + "()"
	}

	// Parse and normalize each field
	fields := parseStructOrUnionFields(fieldsStr)
	var normalizedFields []string
	for _, field := range fields {
		if normalized := normalizeField(field, c); normalized != "" {
			normalizedFields = append(normalizedFields, normalized)
		}
	}

	return typeName + "(" + strings.Join(normalizedFields, ", ") + ")"
}

// findFieldSplitPosition finds the position of the first space that's not inside any nested structures.
// It returns -1 if no valid split position is found.
func findFieldSplitPosition(field string) int {
	var (
		parenCount int // for struct/union fields: STRUCT(field VARCHAR)
		angleCount int // for map types: MAP<INTEGER, VARCHAR>
	)

	for i, char := range field {
		switch char {
		case '(':
			parenCount++
		case ')':
			parenCount--
		case '<':
			angleCount++
		case '>':
			angleCount--
		case ' ':
			// Only split on space if we're not inside any nested structures
			if parenCount == 0 && angleCount == 0 {
				return i
			}
		}
	}
	return -1
}

// normalizeField normalizes a single field in a struct or union type definition.
// It handles field names and their associated types, preserving field names while
// normalizing the type part.
func normalizeField(field string, c *ColumnSchema) string {
	field = strings.TrimSpace(field)
	if field == "" {
		return ""
	}

	// Find the first space that's not inside a nested type
	splitPos := findFieldSplitPosition(field)

	// If we found a valid split position, separate field name and type
	if splitPos != -1 {
		fieldName := strings.TrimSpace(field[:splitPos])
		fieldType := strings.TrimSpace(field[splitPos+1:])
		normalizedType := c.normaliseType(fieldType)
		return fieldName + " " + normalizedType
	}

	// If no valid split found, return the field as is
	return field
}

// parseStructOrUnionFields splits a struct/union field string into individual fields,
// properly handling nested structures.
func parseStructOrUnionFields(fieldsStr string) []string {
	var fields []string
	var currentField string
	var parenCount, angleCount int

	for _, char := range fieldsStr {
		switch char {
		case '(':
			parenCount++
			currentField += string(char)
		case ')':
			parenCount--
			currentField += string(char)
		case '<':
			angleCount++
			currentField += string(char)
		case '>':
			angleCount--
			currentField += string(char)
		case ',':
			if parenCount == 0 && angleCount == 0 {
				if field := strings.TrimSpace(currentField); field != "" {
					fields = append(fields, field)
				}
				currentField = ""
			} else {
				currentField += string(char)
			}
		default:
			currentField += string(char)
		}
	}

	// Add the last field if there is one
	if field := strings.TrimSpace(currentField); field != "" {
		fields = append(fields, field)
	}

	return fields
}

// normalizeWhitespace standardizes whitespace in a string by:
// 1. Removing leading and trailing whitespace
// 2. Converting all newlines, tabs, and carriage returns to spaces
// 3. Collapsing multiple consecutive spaces into a single space
// This ensures consistent whitespace handling for type definitions and field names.
func normalizeWhitespace(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || r == '\r' {
			return ' '
		}
		return r
	}, s)
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	return s
}
