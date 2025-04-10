package table

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
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
	// we only convert one artifact at a time
	// TODO be a bit smarter about this - we could just avoid sending multiple events concurrently)
	conversionMut sync.Mutex
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
	// NOTE: we do not pass error events to CLI - we have added to the status instead
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
	// acquire the conversion mutex to ensure we only convert one artifact at a time
	c.conversionMut.Lock()
	defer c.conversionMut.Unlock()

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

	// notify observers of extraction (for sources which have extractors the source would usually send this event)
	if rowCount > 0 {
		if err := c.Notify(ctx, events.NewArtifactConvertedEvent(c.req.ExecutionId, e.Info, rowCount)); err != nil {
			return fmt.Errorf("error notifying observers of extracted artifact: %w", err)
		}
	}

	// notify observers of the chunk just written (i.e. the un-incremented value)
	return c.onChunk(ctx, chunkCount)
}

func (c *ArtifactConversionCollector) executeConversionQuery(e *events.ArtifactDownloaded, destFile string) (int64, error) {
	// First build query to select source data into temp table and get its columns
	tempTableQuery, err := getTempTableQuery(e.Info.Name, c.table.GetFormat())
	if err != nil {
		slog.Error("ArtifactConversionCollector: error getting temp table query", "error", err)
		return 0, err
	}

	// execute the query to create the temp table and get the columns
	var columnsStr string
	if err := c.db.QueryRow(tempTableQuery).Scan(&columnsStr); err != nil {
		return 0, err
	}
	columns := strings.Split(columnsStr, ",")

	// Now that we have the columns, generate and execute the copy query
	copyQuery := getCopyQuery(c.req.TableName, c.req.PartitionName, destFile, columns, c.req.CustomTableSchema, time.Now())

	// Execute copy query and get row count
	row := c.db.QueryRow(copyQuery)
	var rowCount int64

	if err = row.Scan(&rowCount); err != nil {
		return 0, err
	}

	// now drop the temp table
	if _, err := c.db.Exec("drop table temp_data;"); err != nil {
		slog.Error("ArtifactConversionCollector: error dropping temp table", "error", err)
		return 0, err
	}

	// now update chunk count and row count
	atomic.AddInt32(&c.chunkCount, 1)
	atomic.AddInt64(&c.rowCount, rowCount)
	return rowCount, nil
}

// getTempTableQuery generates the SQL query to create the temp table and return its columns as an array
func getTempTableQuery(sourceFile string, format formats.Format) (string, error) {
	readArtifactSql, err := getReadArtifactSql(sourceFile, format)
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

// getReadArtifactSql generates the SQL to read the artifact based on its format
// (e.g. for delimited format use the duck db command read_csv)
func getReadArtifactSql(sourceFile string, format formats.Format) (string, error) {
	// Get the read function SQL based on format
	var readArtifactSql string
	switch f := format.(type) {
	case *formats.JsonLines:
		jsonOpts := f.GetReadJsonOpts()
		optsString := ""
		if len(jsonOpts) > 0 {
			optsString = fmt.Sprintf(", %s", strings.Join(jsonOpts, ", "))
		}
		readArtifactSql = fmt.Sprintf("read_json('%s'%s)", sourceFile, optsString)
	case *formats.Delimited:
		// Get all CSV options from the format configuration
		csvOpts := f.GetCsvOpts()
		optsString := ""
		if len(csvOpts) > 0 {
			optsString = fmt.Sprintf(", %s", strings.Join(csvOpts, ", "))
		}
		readArtifactSql = fmt.Sprintf("read_csv('%s'%s)", sourceFile, optsString)
	default:
		return "", fmt.Errorf("ArtifactConversionCollector does not support format: %s", f.Identifier())
	}
	return readArtifactSql, nil
}

// getCommonFieldsSelectClauses generates the SQL clauses to select the common fields which we are able to auto populate
func getCommonFieldsSelectClauses(table, partition string, ingestionTime time.Time) []string {
	var commonFieldsClauses = []string{
		fmt.Sprintf("'%s' as tp_table", table),
		fmt.Sprintf("'%s' as tp_partition", partition),
		"gen_random_uuid() as tp_id",
		fmt.Sprintf("'%s' as tp_ingest_timestamp", ingestionTime.Format(time.RFC3339)),
	}

	return commonFieldsClauses
}

// getCopyQuery generates the SQL query to load data from the tamp table, enrich with any additional column mappings
// transform, and copy to JSONL. The row count is returned.
func getCopyQuery(table, partition, destFile string, sourceColumns []string, tableSchema *schema.TableSchema, ingestionTime time.Time) string {
	// Create a map of the existing column names
	sourceColumnMap := utils.SliceToLookup(sourceColumns)

	var selectClauses []string
	// keep track of whether we have mapped tp_index and tp_timestamp - first check do they exist in source data
	_, tpIndexMapped := sourceColumnMap[constants.TpIndex]
	_, tpTimestampMapped := sourceColumnMap[constants.TpTimestamp]

	// Build mapped sourceColumns clauses first
	if len(tableSchema.Columns) > 0 {
		for _, column := range tableSchema.Columns {

			// if we have a transform function, use it
			var sourceExpression string
			switch {
			case column.Transform != "":
				sourceExpression = column.Transform
			case column.SourceName != "":
				sourceExpression = fmt.Sprintf("\"%s\"", column.SourceName)
			default:
				sourceExpression = fmt.Sprintf("\"%s\"", column.ColumnName)
			}

			// coalesce to the default index
			if column.ColumnName == constants.TpIndex {
				tpIndexMapped = true
				sourceExpression = fmt.Sprintf("coalesce(%s, '%s')", sourceExpression, schema.DefaultIndex)
			}
			if column.ColumnName == constants.TpTimestamp {
				tpTimestampMapped = true
			}

			selectClauses = append(selectClauses, fmt.Sprintf(`%s as "%s"`, sourceExpression, column.ColumnName))
			// remove the output name from the map of existing sourceColumns to select
			delete(sourceColumnMap, column.ColumnName)
		}
	}

	// Quote all remaining column names and sort them for consistent order
	var quotedColumns []string
	var remainingColumns []string
	for col := range sourceColumnMap {
		remainingColumns = append(remainingColumns, col)
	}
	sort.Strings(remainingColumns)
	for _, col := range remainingColumns {
		quotedColumns = append(quotedColumns, fmt.Sprintf(`"%s"`, col))
	}
	selectClauses = append(selectClauses, quotedColumns...)

	// Build common fields clauses after mapped sourceColumns
	commonFieldsClauses := getCommonFieldsSelectClauses(table, partition, ingestionTime)
	selectClauses = append(selectClauses, commonFieldsClauses...)

	// if we have a mapping for tp_timestamp, add tp_date as well
	if tpTimestampMapped {
		// Add tp_date after tp_timestamp is defined
		selectClauses = append(selectClauses, `case
		when tp_timestamp is not null
		then date_trunc('day', tp_timestamp::timestamp)
	end as tp_date`)
	}

	// if tp_index is not mapped, add the default index
	if !tpIndexMapped {
		selectClauses = append(selectClauses, fmt.Sprintf("'%s' as tp_index", schema.DefaultIndex))
	}

	// Build the query
	query := fmt.Sprintf(`-- Transform and copy data to destination
copy (select
    %s
from temp_data)
to '%s' (
    format json
);

-- Get row count
select count(*) as row_count from temp_data;`,
		strings.Join(selectClauses, ",\n    "),
		destFile)

	return query
}
