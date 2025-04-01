package collectors

import (
	"fmt"
	"strings"
)

type Column struct {
	ColumnName          string
	SourceName          string
	AutoMapSourceFields bool
}

type TableSchema struct {
	Columns             []Column
	AutoMapSourceFields bool
}

// GetTempTableAndDropStatementsQuery builds the query to create the temp table and get drop statements
func GetTempTableAndDropStatementsQuery(sourcePath string, tableSchema *TableSchema) string {
	// Build list of mapped column names
	var mappedColumns []string
	for _, column := range tableSchema.Columns {
		if column.SourceName != "" {
			mappedColumns = append(mappedColumns, column.SourceName)
		}
	}

	// Build the query
	query := fmt.Sprintf(`CREATE TABLE temp_data AS 
SELECT * FROM read_json_auto('%s');

WITH clashing_columns AS (
    SELECT unnest(%s) AS source_name
    INTERSECT
    SELECT name FROM pragma_table_info('temp_data')
),
drop_statements AS (
    SELECT 
        CASE 
            WHEN EXISTS (SELECT 1 FROM clashing_columns)
            THEN 'ALTER TABLE temp_data DROP COLUMN "' || source_name || '";'
            ELSE NULL
        END AS drop_sql
    FROM clashing_columns
)
SELECT * FROM drop_statements WHERE drop_sql IS NOT NULL;`,
		sourcePath,
		fmt.Sprintf("['%s']", strings.Join(mappedColumns, "', '")))

	return query
}

// GetTransformationQuery builds the query to transform the data
func GetTransformationQuery(tableSchema *TableSchema) string {
	var selectClauses []string

	// Add standard fields
	selectClauses = append(selectClauses,
		"'default' AS tp_index",
		"CASE WHEN timestamp IS NOT NULL THEN date_trunc('day', timestamp::TIMESTAMP) END AS tp_date",
		"gen_random_uuid() AS tp_id",
		"CURRENT_TIMESTAMP AS tp_ingest_timestamp")

	// Add mapped columns
	for _, column := range tableSchema.Columns {
		if column.SourceName != "" {
			selectClauses = append(selectClauses, fmt.Sprintf("%s as %s", column.SourceName, column.ColumnName))
		}
	}

	// Add remaining columns
	selectClauses = append(selectClauses, "*")

	return fmt.Sprintf("SELECT %s FROM temp_data", strings.Join(selectClauses, ", "))
}
