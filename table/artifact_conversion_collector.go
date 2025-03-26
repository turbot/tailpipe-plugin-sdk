package table

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/filepaths"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	return c.table.GetSchema()
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
	query, err := c.getQuery(e.Info.Name, destFile)
	if err != nil {
		slog.Error("ArtifactConversionCollector: error getting query", "error", err)
		return 0, err
	}
	// execute the query
	row := c.db.QueryRow(query)
	var rowCount int64

	if err = row.Scan(&rowCount); err != nil {
		return 0, err
	}

	// now update chunk count and row count
	atomic.AddInt32(&c.chunkCount, 1)
	atomic.AddInt64(&c.rowCount, rowCount)
	return rowCount, nil
}

func (c *ArtifactConversionCollector) getQuery(sourceFile string, destFile string) (string, error) {

	// build the select clause
	// use the raw table schema from the request, rather than the CustomTable schema, which includes all common columns
	tableSchema := c.req.CustomTableSchema

	var selectClauses []string
	// if we are automapping then we select all columns, as well as mapped columns
	if tableSchema.AutoMapSourceFields {
		selectClauses = append(selectClauses, "*")
	}

	for _, column := range tableSchema.Columns {
		selectClauses = append(selectClauses, fmt.Sprintf("%s as %s", column.SourceName, column.ColumnName))
	}
	selectString := strings.Join(selectClauses, ",\n    ")

	// build the read function sql based on the format
	getReadArtifactSql, err := c.getReadArtifactSql(sourceFile)
	if err != nil {
		return "", err
	}

	queryFormat := `create temp table temp_data as 
select
    %s
from %s;

copy temp_data to '%s' (
    format json
);

select count(*) as row_count from temp_data;`

	return fmt.Sprintf(queryFormat, selectString, getReadArtifactSql, destFile), nil
}

func (c *ArtifactConversionCollector) getReadArtifactSql(sourceFile string) (string, error) {
	switch f := c.table.GetFormat().(type) {
	case *formats.JsonLines:
		return fmt.Sprintf("read_json('%s')", sourceFile), nil
	case *formats.Delimited:
		options := f.GetCsvOpts()
		return fmt.Sprintf("read_csv('%s', %s)", sourceFile, strings.Join(options, ", ")), nil
	default:
		return "", fmt.Errorf("ArtifactConversionCollector does not support format: %s", f.Identifier())
	}
}
