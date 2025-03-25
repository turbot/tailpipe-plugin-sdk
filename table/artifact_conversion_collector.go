package table

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
	"sync"
	"time"
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
	chunkCount          int64
}

func NewArtifactConversionCollector(table CustomTable) *ArtifactConversionCollector {
	return &ArtifactConversionCollector{
		table: table,
	}
}

func (c *ArtifactConversionCollector) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req

	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata := c.getSourceMetadata()

	if err := c.initSource(ctx, req, sourceMetadata); err != nil {
		return err
	}

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

// GetSchema returns the schema of the table if available
// for dynamic tables, the schema is only available at this if the config contains a schema
func (c *ArtifactConversionCollector) GetSchema() (*schema.TableSchema, error) {
	return c.table.GetSchema()
}

//func (c *ArtifactConversionCollector) initialiseConfig(tableConfigData types.ConfigData) error {
//	// default to empty config
//	//cfg := utils.InstanceOf()
//
//	if len(tableConfigData.GetHcl()) > 0 {
//
//		cfg, err := parse.ParseConfig(tableConfigData)
//		if err != nil {
//			return fmt.Errorf("error parsing config: %w", err)
//		}
//
//		slog.Info("tableName RowSourceImpl: config parsed", "config", c)
//		c.Format = cfg
//	}
//
//	// validate config
//	if err := c.Format.Validate(); err != nil {
//		return fmt.Errorf("invalid partition config: %w", err)
//	}
//
//	return nil
//}
//
//func (c *ArtifactConversionCollector) initialiseFormat(tableConfigData types.ConfigData) error {
//	// default to empty config
//	//cfg := utils.InstanceOf()
//
//	if len(tableConfigData.GetHcl()) > 0 {
//		cfg, err := parse.ParseConfig(tableConfigData)
//		if err != nil {
//			return fmt.Errorf("error parsing config: %w", err)
//		}
//
//		slog.Info("tableName RowSourceImpl: config parsed", "config", c)
//		c.Format = cfg
//	}
//
//	// validate config
//	if err := c.Format.Validate(); err != nil {
//		return fmt.Errorf("invalid partition config: %w", err)
//	}
//
//	return nil
//}

// Collect executes the collection process. Tell our source to start collection
func (c *ArtifactConversionCollector) Collect(ctx context.Context) (int, int, error) {
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
	return int(c.rowCount), int(c.chunkCount), nil

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
		// set a null loader so we don't receive row events - instead we implement ArtifactToJsonConverter
		// to convert the artifact to JSONL directly
		Options: []row_source.RowSourceOption{artifact_source.WithArtifactLoader(artifact_loader.NewNullLoader())},
	}
}

func (c *ArtifactConversionCollector) handleArtifactDownloaded(ctx context.Context, e *events.ArtifactDownloaded) error {
	// TODO K
	//executionId, err := context_values.ExecutionIdFromContext(ctx)
	//if err != nil {
	//	return err
	//}
	//
	//// get chunk count
	//c.rowBufferLock.Lock()
	//chunkNumber := c.chunkCountMap[e.ExecutionId]
	//c.rowBufferLock.Unlock()
	//
	//
	//chunkCount, rowCount, err := q.ArtifactToJSON(ctx, e.Info.Name, executionId, chunkNumber, c.Config)
	//if err != nil {
	//	return err
	//}
	//
	//// TODO onchunks
	//
	//// update rows and chunks written
	//c.rowBufferLock.Lock()
	//c.rowCountMap[e.ExecutionId] += rowCount
	//c.chunkCountMap[e.ExecutionId]+= chunkCount
	//c.rowBufferLock.Unlock()
	slog.Info("ArtifactConversionCollector: artifact downloaded", "artifact", e.Info.Name, "executionId", e.ExecutionId)
	//TODO K delete local artifact
	return nil

}
