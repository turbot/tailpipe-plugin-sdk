package table

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"golang.org/x/exp/maps"
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
		c.Delimiter = &delimiter
	}
}

func WithCsvComment(comment string) CsvToJsonOpts {
	return func(c *CsvTableConfig) {
		c.Comment = &comment
	}
}

func WithCsvSchema(schema *schema.RowSchema) CsvToJsonOpts {
	return func(c *CsvTableConfig) {
		c.Schema = schema
	}
}

type CsvTableConfig struct {
	HeaderMode CsvHeaderMode
	Delimiter  *string
	Comment    *string
	Schema     *schema.RowSchema
}

func CsvToJsonQuery(sourceFile, destFile string, mappings map[string]string, opts ...CsvToJsonOpts) string {
	// Initialize the default configuration
	config := &CsvTableConfig{
		HeaderMode: CsvHeaderModeAuto, // Default to assuming a header row
	}

	// Apply the options to the configuration
	for _, opt := range opts {
		opt(config)
	}

	// Build the DuckDB query components
	var readCsvOpts []string
	readCsvOpts = append(readCsvOpts, fmt.Sprintf("'%s'", sourceFile))

	// Add DELIM option
	if config.Delimiter != nil {
		readCsvOpts = append(readCsvOpts, fmt.Sprintf("DELIM '%s'", *config.Delimiter))
	}

	// Add HEADER option
	switch config.HeaderMode {
	case CsvHeaderModeOn:
		readCsvOpts = append(readCsvOpts, "HEADER TRUE")
	case CsvHeaderModeOff:
		readCsvOpts = append(readCsvOpts, "HEADER FALSE")
	}

	// Add COMMENT option
	if config.Comment != nil {
		readCsvOpts = append(readCsvOpts, fmt.Sprintf("COMMENT '%s'", *config.Comment))
	}

	// Start building the query
	query := "COPY ("

	// the mappings provided are for tp_ fields - we select mapped columns with '<source_name> AS <mapped_name>
	// e.g. SELECT index AS tp_index, log_tims as tp_timestamp, * FROM read_csv(...)
	// build the mapped column SELECT clause
	mappedColumnSelectString := getMappedColumnSelect(mappings)

	// if a FULL schema is provided, use it to build the remaining columns to select - otherwise select all columns (*)
	columnSelectString := getSchemaColumnSelect(config.Schema)
	// Use the mapped columns
	query += fmt.Sprintf("SELECT %s%s FROM read_csv(%s)", mappedColumnSelectString, columnSelectString, strings.Join(readCsvOpts, ", "))

	// Close COPY and specify the output file and format
	query += fmt.Sprintf(") TO '%s' (FORMAT JSON) RETURNING COUNT(*) AS row_count;", destFile)

	return query
}

func getSchemaColumnSelect(rowSchema *schema.RowSchema) string {
	if rowSchema == nil || len(rowSchema.Columns) == 0 || rowSchema.Mode != schema.ModeFull {
		return "*"
	}

	var columns []string
	for _, column := range rowSchema.Columns {
		columns = append(columns, column.ColumnName)
	}

	return strings.Join(columns, ", ")
}

func getMappedColumnSelect(mappings map[string]string) string {
	if len(mappings) == 0 {
		return ""
	}
	var mappedColumns []string

	// Sort the keys for deterministic ordering
	keys := maps.Keys(mappings)
	sort.Strings(keys)
	for _, destField := range keys {
		sourceField := mappings[destField]
		mappedColumns = append(mappedColumns, fmt.Sprintf("%s AS %s", sourceField, destField))
	}

	return strings.Join(mappedColumns, ", ") + ", "
}
