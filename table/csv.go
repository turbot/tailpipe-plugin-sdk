package table

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"sort"
	"strings"
)

type CsvHeaderMode string

const (
	CsvHeaderModeAuto CsvHeaderMode = "auto"
	CsvHeaderModeOff  CsvHeaderMode = "off"
	CsvHeaderModeOn   CsvHeaderMode = "on"
)

// opts

type CsvToJsonOpts func(*CsvTableConfig)

func WithCsvHeaderMode(headerMode CsvHeaderMode) CsvToJsonOpts {
	return func(c *CsvTableConfig) {
		c.HeaderMode = headerMode
	}
}

func WithCsvDelimiter(delimiter string) CsvToJsonOpts {
	return func(c *CsvTableConfig) {
		c.Delimiter = delimiter
	}
}

func WithCsvComment(comment string) CsvToJsonOpts {
	return func(c *CsvTableConfig) {
		c.Comment = comment
	}
}

func WithCsvSchema(schema *schema.RowSchema) CsvToJsonOpts {
	return func(c *CsvTableConfig) {
		c.Schema = schema
	}
}

type CsvTableConfig struct {
	HeaderMode CsvHeaderMode
	Delimiter  string
	Comment    string
	Schema     *schema.RowSchema
}

func CsvToJsonQuery(sourceFile, destFile string, mappings map[string]string, opts ...CsvToJsonOpts) string {
	// Initialize the default configuration
	config := &CsvTableConfig{
		HeaderMode: CsvHeaderModeOn, // Default to assuming a header row
		Delimiter:  ",",             // Default delimiter
		Comment:    "",              // No comment character by default
		Schema:     nil,             // No schema by default
	}

	// Apply the options to the configuration
	for _, opt := range opts {
		opt(config)
	}

	// Build the DuckDB query components
	var readCsvOpts []string
	readCsvOpts = append(readCsvOpts, fmt.Sprintf("'%s'", sourceFile))

	// Add DELIM option
	if config.Delimiter != "" {
		readCsvOpts = append(readCsvOpts, fmt.Sprintf("DELIM '%s'", config.Delimiter))
	}

	// Add HEADER option
	switch config.HeaderMode {
	case CsvHeaderModeOn:
		readCsvOpts = append(readCsvOpts, "HEADER TRUE")
	case CsvHeaderModeOff:
		readCsvOpts = append(readCsvOpts, "HEADER FALSE")
	}

	// Add COMMENT option
	if config.Comment != "" {
		readCsvOpts = append(readCsvOpts, fmt.Sprintf("COMMENT '%s'", config.Comment))
	}

	// Start building the query
	query := "COPY ("

	// Dynamically build the SELECT clause based on mappings
	columns := []string{}

	// Sort the keys for deterministic ordering
	keys := make([]string, 0, len(mappings))
	for key := range mappings {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Build the column mappings
	for _, destField := range keys {
		sourceField := mappings[destField]
		columns = append(columns, fmt.Sprintf("%s AS %s", sourceField, destField))
	}

	if len(columns) > 0 {
		// Use the mapped columns
		query += fmt.Sprintf("SELECT %s FROM read_csv(%s)", strings.Join(columns, ", "), strings.Join(readCsvOpts, ", "))
	} else {
		// Default to SELECT * if no mappings are provided
		query += fmt.Sprintf("SELECT * FROM read_csv(%s)", strings.Join(readCsvOpts, ", "))
	}

	// Close COPY and specify the output file and format
	query += fmt.Sprintf(") TO '%s' (FORMAT JSON) RETURNING COUNT(*) AS row_count;", destFile)

	return query
}
