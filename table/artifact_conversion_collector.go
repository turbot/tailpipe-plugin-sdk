package table

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/filepaths"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// ArtifactConversionCollector is a collector that converts artifacts directly to JSONL
// S is the table config type
type ArtifactConversionCollector struct {
	observable.ObservableImpl

	table  CustomTable
	req    *types.CollectRequest
	source row_source.RowSource

	// wait group to wait for all artifacts to be processed
	// this is incremented each time we receive an artifact event and decremented when we have processed it
	artifactWg sync.WaitGroup
	status     *events.Status

	lastStatusEventTime time.Time
	rowCount            int64
	chunkCount          int32
	destPath            string
	executionId         string
	db                  *sql.DB
}

func NewArtifactConversionCollector(table CustomTable) *ArtifactConversionCollector {
	return &ArtifactConversionCollector{
		table: table,
	}
}

func (c *ArtifactConversionCollector) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	c.executionId = executionId
	db, err := c.initDb()
	if err != nil {
		return err
	}
	c.db = db

	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata := c.getSourceMetadata()

	if err := c.initSource(ctx, req, sourceMetadata); err != nil {
		return err
	}

	// if the plugin overrides this function it must call the base implementation
	// get JSONL path
	jsonPath, err := filepaths.EnsureJSONLPath(req.CollectionTempDir)
	if err != nil {
		return fmt.Errorf("error getting JSONL path: %w", err)
	}
	c.destPath = jsonPath

	// TODO #validate validate no extractor
	// TODO #validate validate table name does not clash

	slog.Info("Start collection")

	return nil
}

func (c *ArtifactConversionCollector) Identifier() string {
	return c.table.Identifier()
}

// GetFromTime returns the 'resolved' from time of the source
func (c *ArtifactConversionCollector) GetFromTime() *row_source.ResolvedFromTime {
	return c.source.GetFromTime()
}

// GetSchema returns the schema of the table
func (c *ArtifactConversionCollector) GetSchema() (*schema.TableSchema, error) {
	return c.table.GetSchema()
}

// Collect executes the collection process. Tell our source to start collection
func (c *ArtifactConversionCollector) Collect(ctx context.Context) (int64, int32, error) {
	// create empty status event#
	c.status = events.NewStatusEvent(c.req.ExecutionId)

	// tell our source to collect
	// this is a blocking call, but we will receive and process row events during the execution
	err := c.source.Collect(ctx)
	if err != nil {
		return 0, 0, err
	}

	slog.Info("Source collection complete - waiting for enrichment")
	c.artifactWg.Wait()
	defer slog.Info("Enrichment complete")

	// notify observers of final status
	if err := c.NotifyObservers(ctx, c.status); err != nil {
		slog.Error("tableName RowSourceImpl: error notifying observers of status", "error", err)
	}

	// return the number of rows processed
	return c.rowCount, c.chunkCount, nil

}

// Notify implements observable.Observer
// it handles all events which collectorFuncMap may receive (these will all come from the source)
func (c *ArtifactConversionCollector) Notify(ctx context.Context, event events.Event) error {
	// update the status counts
	c.updateStatus(ctx, event)

	switch e := event.(type) {
	case *events.ArtifactDownloaded:
		// handle artifact downloaded event - we only act on this if the table implements ArtifactToJsonConverter
		return c.handleArtifactDownloaded(e)

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

func (c *ArtifactConversionCollector) initSource(ctx context.Context, req *types.CollectRequest, sourceMetadata *SourceMetadata[*types.DynamicRow]) error {
	params := &row_source.RowSourceParams{
		SourceConfigData:    req.SourceData,
		ConnectionData:      req.ConnectionData,
		CollectionStatePath: req.CollectionStatePath,
		From:                req.From,
		CollectionTempDir:   req.CollectionTempDir,
	}

	// ask factory to create and initialise the source for us
	// NOTE: we pass the original
	source, err := row_source.Factory.GetRowSource(ctx, params, sourceMetadata.Options...)
	if err != nil {
		return err
	}

	c.source = source
	// there will not be a mapper

	// add ourselves as an observer to our Source
	return c.source.AddObserver(c)

}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (c *ArtifactConversionCollector) updateStatus(ctx context.Context, e events.Event) {
	c.status.Update(e)

	// send a status event periodically
	if time.Since(c.lastStatusEventTime) > events.StatusUpdateInterval {
		// notify observers
		if err := c.NotifyObservers(ctx, c.status); err != nil {
			slog.Error("tableName RowSourceImpl: error notifying observers of status", "error", err)
		}
		// update lastStatusEventTime
		c.lastStatusEventTime = time.Now()
	}
}

func (c *ArtifactConversionCollector) getSourceMetadata() *SourceMetadata[*types.DynamicRow] {
	return &SourceMetadata[*types.DynamicRow]{
		SourceName: constants.ArtifactSourceIdentifier,
		// set a null loader so we don't receive row events - instead we handle the artifact downloaded event
		// to convert the artifact to JSONL directly
		Options: []row_source.RowSourceOption{artifact_source.WithArtifactLoader(artifact_loader.NewNullLoader())},
	}
}

func (c *ArtifactConversionCollector) handleArtifactDownloaded(e *events.ArtifactDownloaded) error {
	query, err := c.getQuery(e.Info.Name)
	if err != nil {
		slog.Error("ArtifactConversionCollector: error getting query", "error", err)
		return err
	}
	// execute the query
	row := c.db.QueryRow(query)
	var rowCount int64

	if err = row.Scan(&rowCount); err != nil {
		return err
	}

	// now update chunk count and row count
	atomic.AddInt32(&c.chunkCount, 1)
	atomic.AddInt64(&c.rowCount, rowCount)

	slog.Info("ArtifactConversionCollector: artifact converted", "artifact", e.Info.Name, "rowCount", rowCount, "chunkCount", c.chunkCount)
	//TODO K delete local artifact

	return nil
}

func (c *ArtifactConversionCollector) getQuery(sourceFile string) (string, error) {
	chunkNumber := atomic.LoadInt32(&c.chunkCount)
	// generate the filename
	destFile := filepath.Join(c.destPath, ExecutionIdToJsonlFileName(c.executionId, chunkNumber))

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
