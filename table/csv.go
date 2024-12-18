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

func WithMappings(mappings map[string]string) CsvToJsonOpts {
	return func(c *CsvTableConfig) {
		c.Mappings = mappings
	}
}

type CsvTableConfig struct {
	HeaderMode CsvHeaderMode
	Delimiter  *string
	Comment    *string
	Schema     *schema.RowSchema
	Mappings   map[string]string
}

func GetReadCsvChunkQueryFormat(sourceFile string, opts ...CsvToJsonOpts) string {
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

	// Add skip option
	readCsvOpts = append(readCsvOpts, `SKIP %d`)

	// the mappings provided are for tp_ fields - we select mapped columns with '<source_name> AS <mapped_name>
	// e.g. SELECT index AS tp_index, log_tims as tp_timestamp, * FROM read_csv(...)
	// build the mapped column SELECT clause
	mappedColumnSelectString := getMappedColumnSelect(config.Mappings)

	// if a FULL schema is provided, use it to build the remaining columns to select - otherwise select all columns (*)
	columnSelectString := getSchemaColumnSelect(config.Schema)

	// Use the mapped columns
	return fmt.Sprintf("SELECT %s%s FROM read_csv(%s) LIMIT %d", mappedColumnSelectString, columnSelectString, strings.Join(readCsvOpts, ", "), JSONLChunkSize)
}

func getSchemaColumnSelect(rowSchema *schema.RowSchema) string {
	if rowSchema == nil || len(rowSchema.Columns) == 0 || rowSchema.AutoMapSourceFields {
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
