package table

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/filepaths"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// ArtifactConversionCollector is a collector that converts artifacts directly to JSONL
// S is the table config type
type ArtifactConversionCollector struct {
	CollectorImpl[*types.DynamicRow]

	table CustomTable

	destPath string
	db       *sql.DB
}

func NewArtifactConversionCollector(table CustomTable) *ArtifactConversionCollector {
	return &ArtifactConversionCollector{
		table: table,
	}
}

func (c *ArtifactConversionCollector) Init(ctx context.Context, req *types.CollectRequest) error {
	// store request
	c.req = req

	// create a db connection
	db, err := c.initDb()
	if err != nil {
		return err
	}
	c.db = db

	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata := c.getSourceMetadata()

	// TODO #validate validate no extractor
	// TODO #validate validate table name does not clash

	// create the source
	if err := c.initSource(ctx, req, sourceMetadata); err != nil {
		return err
	}

	// add ourselves as an observer to our source
	if err := c.source.AddObserver(c); err != nil {
		return err
	}

	// ensure dest path exists
	jsonPath, err := filepaths.EnsureJSONLPath(req.CollectionTempDir)
	if err != nil {
		return fmt.Errorf("error getting JSONL path: %w", err)
	}
	c.destPath = jsonPath
	return nil
}

// Close closes the collector and releases any resources
func (c *ArtifactConversionCollector) Close() {
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			slog.Error("ArtifactConversionCollector: error closing db", "error", err)
		}
	}
}

func (c *ArtifactConversionCollector) Identifier() string {
	return c.table.Identifier()
}

// GetSchema returns the schema of the table
func (c *ArtifactConversionCollector) GetSchema() (*schema.TableSchema, error) {
	s, err := c.table.GetSchema()
	if err != nil {
		return nil, err
	}
	// we have already mapped source fields to output fields, so clear the source fields
	return s.WithSourceFieldsCleared(), nil
}

// Notify implements observable.Observer
// it handles all events which collectorFuncMap may receive (these will all come from the source)
func (c *ArtifactConversionCollector) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	c.updateStatus(ctx, event)

	switch e := event.(type) {
	case *events.ArtifactDownloaded:
		// handle artifact downloaded event - we only act on this if the table implements ArtifactToJsonConverter
		return c.handleArtifactDownloaded(ctx, e)

	case *events.Error:
		slog.Error("ArtifactConversionCollector: error event received", "error", e.Err)
		return c.NotifyObservers(context.Background(), e)
	default:
		// ignore
		return nil
	}
}

func (c *ArtifactConversionCollector) initDb() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("error opening duckdb: %w", err)
	}
	// instrall JSON extension
	if _, err := db.Exec("install 'json'; load 'json';"); err != nil {
		return nil, fmt.Errorf("error installing json extension: %w", err)
	}
	return db, nil

}

func (c *ArtifactConversionCollector) getSourceMetadata() *SourceMetadata[*types.DynamicRow] {
	return &SourceMetadata[*types.DynamicRow]{
		SourceName: constants.ArtifactSourceIdentifier,
		// set a null loader so we don't receive row events - instead we handle the artifact downloaded event
		// to convert the artifact to JSONL directly
		Options: []row_source.RowSourceOption{artifact_source.WithArtifactLoader(artifact_loader.NewNullLoader())},
	}
}

func (c *ArtifactConversionCollector) handleArtifactDownloaded(ctx context.Context, e *events.ArtifactDownloaded) error {
	// increment the collection wait group
	c.collectionWg.Add(1)
	defer c.collectionWg.Done()

	// load the current chunk count
	chunkCount := atomic.LoadInt32(&c.chunkCount)
	// generate the filename
	destFile := filepath.Join(c.destPath, ExecutionIdToJsonlFileName(c.req.ExecutionId, chunkCount))

	rowCount, err := c.executeConversionQuery(e, destFile)
	if err != nil {
		return err
	}

	slog.Info("ArtifactConversionCollector: artifact converted", "artifact", e.Info.Name, "rowCount", rowCount, "chunkCount", c.chunkCount)

	//TODO K delete local artifact

	// notify observers of the chunk just written (i.e. the un-incremented value)
	return c.onChunk(ctx, chunkCount)
}

func (c *ArtifactConversionCollector) executeConversionQuery(e *events.ArtifactDownloaded, destFile string) (int64, error) {
	// First create temp table and get its columns
	tempTableQuery, err := c.getTempTableQuery(e.Info)
	if err != nil {
		return 0, err
	}

	// the temp table read the artifact into a temp table and returns the columns as a string
	// execute this first to get the columns
	var columnsStr string
	if err := c.db.QueryRow(tempTableQuery).Scan(&columnsStr); err != nil {
		return 0, err
	}
	columns := strings.Split(columnsStr, ",")

	// Now that we have the columns, generate and execute the copy query
	copyQuery := c.getCopyQuery(destFile, columns, e.Info)
	row := c.db.QueryRow(copyQuery)
	var rowCount int64

	if err := row.Scan(&rowCount); err != nil {
		return 0, err
	}

	// now update chunk count and row count
	atomic.AddInt32(&c.chunkCount, 1)
	atomic.AddInt64(&c.rowCount, rowCount)
	return rowCount, nil
}

func (c *ArtifactConversionCollector) getReadArtifactSql(info *types.DownloadedArtifactInfo) (string, error) {
	var readArtifactSql string
	switch f := c.table.GetFormat().(type) {
	case *formats.JsonLines:
		readArtifactSql = fmt.Sprintf("read_json('%s')", info.Name)
	case *formats.Delimited:
		options := f.GetCsvOpts()
		readArtifactSql = fmt.Sprintf("read_csv('%s', %s)", info.Name, strings.Join(options, ", "))
	default:
		return "", fmt.Errorf("ArtifactConversionCollector does not support format: %s", f.Identifier())
	}
	return readArtifactSql, nil
}

// getTempTableQuery generates the SQL query to create the temp table and return its columns as an array
func (c *ArtifactConversionCollector) getTempTableQuery(info *types.DownloadedArtifactInfo) (string, error) {
	// Get the read function SQL based on format
	readArtifactSql, err := c.getReadArtifactSql(info)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`-- Create temp table from source data
create temp table temp_data as
select *
from %s;

-- Return the columns as an array
select string_agg(name, ',') from pragma_table_info('temp_data');`,
		readArtifactSql), nil
}

// getCommonFieldsSelectClauses generates the SQL clauses for common fields
func (c *ArtifactConversionCollector) getCommonFieldsSelectClauses(sourceInfo *types.DownloadedArtifactInfo) []string {
	var commonFieldsClauses []string = []string{
		fmt.Sprintf("'%s' as tp_table", sourceInfo.SourceEnrichment.CommonFields.TpTable),
		fmt.Sprintf("'%s' as tp_partition", sourceInfo.SourceEnrichment.CommonFields.TpPartition),
		"case\n        when tp_timestamp is not null\n        then date_trunc('day', tp_timestamp::TIMESTAMP)\n    end as tp_date",
		"gen_random_uuid() as tp_id",
		fmt.Sprintf("'%s' as tp_ingest_timestamp", time.Now().Format(time.RFC3339)),
	}

	// Check if tp_index is already mapped
	tpIndexMapped := false
	for sourceName := range c.req.CustomTableSchema.Columns {
		if c.req.CustomTableSchema.Columns[sourceName].SourceName == "tp_index" {
			tpIndexMapped = true
			break
		}
	}

	// if there is not a mapping for tp_index, add the default index
	if !tpIndexMapped {
		commonFieldsClauses = append(commonFieldsClauses, fmt.Sprintf("coalesce(tp_index, '%s') as tp_index", schema.DefaultIndex))
	}

	return commonFieldsClauses
}

// getCopyQuery generates the SQL query to transform, copy and count the data
func (c *ArtifactConversionCollector) getCopyQuery(destFile string, columns []string, sourceInfo *types.DownloadedArtifactInfo) string {
	// Build mapped columns clause
	var mappedColumnsClause string
	if len(c.req.CustomTableSchema.Columns) > 0 {
		var clauses []string
		// Get sorted source names for deterministic output
		sourceNames := make([]string, 0, len(c.req.CustomTableSchema.Columns))
		for _, column := range c.req.CustomTableSchema.Columns {
			if column.SourceName != "" {
				sourceNames = append(sourceNames, column.SourceName)
			}
		}
		sort.Strings(sourceNames)

		for _, sourceName := range sourceNames {
			var columnName string
			for _, column := range c.req.CustomTableSchema.Columns {
				if column.SourceName == sourceName {
					columnName = column.ColumnName
					break
				}
			}
			if sourceName != "" && sourceName != columnName {
				clauses = append(clauses, fmt.Sprintf("%s as %s", sourceName, columnName))
			} else {
				clauses = append(clauses, columnName)
			}
		}
		mappedColumnsClause = strings.Join(clauses, ",\n    ")
	}

	// Build remaining columns clause if auto-mapping is enabled
	var remainingColumnsClause string
	if c.req.CustomTableSchema.AutoMapSourceFields {
		// Create a map of columns to exclude
		excludeColumns := make(map[string]bool)
		excludeColumns["tp_index"] = true
		excludeColumns["tp_timestamp"] = true
		excludeColumns["tp_date"] = true
		excludeColumns["tp_id"] = true
		excludeColumns["tp_ingest_timestamp"] = true
		for _, column := range c.req.CustomTableSchema.Columns {
			if column.SourceName != "" {
				excludeColumns[column.SourceName] = true
			}
		}

		// If we have columns from the temp table, use them
		// Filter out excluded columns
		var remainingColumns []string
		for _, col := range columns {
			if !excludeColumns[col] {
				remainingColumns = append(remainingColumns, col)
			}
		}

		if len(remainingColumns) > 0 {
			sort.Strings(remainingColumns)
			remainingColumnsSelect := strings.Join(remainingColumns, ",\n    ")
			// Only add the comma if we have mapped columns
			if mappedColumnsClause != "" {
				remainingColumnsClause = ",\n    " + remainingColumnsSelect
			} else {
				remainingColumnsClause = remainingColumnsSelect
			}
		}
	}

	// Build common fields clauses
	commonFieldsClauses := c.getCommonFieldsSelectClauses(sourceInfo)

	// Build the query
	query := fmt.Sprintf(`-- Transform and copy data to destination
copy (select
    %s%s%s
from temp_data)
to '%s' (
    format json
);

-- Get row count
select count(*) as row_count from temp_data;`,
		mappedColumnsClause,
		remainingColumnsClause,
		func() string {
			if mappedColumnsClause != "" || remainingColumnsClause != "" {
				return ",\n    " + strings.Join(commonFieldsClauses, ",\n    ")
			}
			return strings.Join(commonFieldsClauses, ",\n    ")
		}(),
		destFile)

	return query
}

// getConversionQuery creates SQL queries to convert a source file to a destination JSON file
// It returns two queries:
// 1. A query to create the temp table and handle drop statements
// 2. A query to copy and count the data
func (c *ArtifactConversionCollector) getConversionQuery(sourceInfo *types.DownloadedArtifactInfo, destFile string, tableSchema *schema.TableSchema, format formats.Format) (string, string, error) {
	// Get the read function SQL based on format
	var readArtifactSql string
	switch f := format.(type) {
	case *formats.JsonLines:
		readArtifactSql = fmt.Sprintf("read_json('%s')", sourceInfo.Name)
	case *formats.Delimited:
		options := f.GetCsvOpts()
		readArtifactSql = fmt.Sprintf("read_csv('%s', %s)", sourceInfo.Name, strings.Join(options, ", "))
	default:
		return "", "", fmt.Errorf("ArtifactConversionCollector does not support format: %s", f.Identifier())
	}

	// Build map of mapped columns
	mappedColumns := make(map[string]string)
	for _, column := range tableSchema.Columns {
		if column.SourceName != "" {
			mappedColumns[column.SourceName] = column.ColumnName
		}
	}

	// Get the temp table query
	tempTableQuery := fmt.Sprintf(`-- Create temp table from source data
create temp table temp_data as
select *
from %s;

-- Return the columns as an array
select string_agg(name, ',') from pragma_table_info('temp_data');`, readArtifactSql)

	// Get the copy query - we'll get the columns from the temp table when we execute it
	copyQuery := c.getCopyQuery(destFile, nil, sourceInfo)

	return tempTableQuery, copyQuery, nil
}

func (c *ArtifactConversionCollector) onChunk(ctx context.Context, chunkNumber int32) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}

	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber)

	if err = c.NotifyObservers(ctx, e); err != nil {
		return fmt.Errorf("error notifying observers of chunk: %w", err)
	}

	// tell source to save collection state
	if err := c.source.SaveCollectionState(); err != nil {
		return fmt.Errorf("error saving collection state: %w", err)
	}
	return nil
}
