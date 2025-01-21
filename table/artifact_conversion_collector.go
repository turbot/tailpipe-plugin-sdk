package table

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"log/slog"
	"sync"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// ArtifactConversionCollector is a collector that converts artifacts directly to JSONL
// S is the table config type
type ArtifactConversionCollector[S parse.Config] struct {
	observable.ObservableImpl

	source row_source.RowSource

	tableName string
	// the source format
	formatData *proto.ConfigData
	// the table config
	Format S

	// wait group to wait for all rows to be processed
	// this is incremented each time we receive a row event and decremented when we have processed it
	status              *events.Status
	lastStatusEventTime time.Time
	statusLock          sync.RWMutex
	enrichTiming        types.Timing
	req                 *types.CollectRequest
}

func (c *ArtifactConversionCollector[S]) UpdateCollectionState(ctx context.Context, request *types.CollectRequest) error {
	//TODO implement me
	panic("implement me")
}

func NewArtifactConversionCollector[S parse.Config](tableName string, formatData *proto.ConfigData) *ArtifactConversionCollector[S] {
	return &ArtifactConversionCollector[S]{
		tableName:  tableName,
		formatData: formatData,
	}
}

func (c *ArtifactConversionCollector[S]) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req

	// TODO #validate validate no extractor
	// TODO #validate validate table name does not clash

	slog.Info("tableName RowSourceImpl: Collect", "table", c.tableName)
	if err := c.initSource(ctx, req.SourceData, req.ConnectionData); err != nil {
		return err
	}
	slog.Info("Start collection")

	return nil
}

func (c *ArtifactConversionCollector[S]) Identifier() string {
	return c.tableName
}

// GetSchema returns the schema of the table if available
// for dynamic tables, the schema is only available at this if the config contains a schema
func (c *ArtifactConversionCollector[S]) GetSchema() (*schema.RowSchema, error) {
	var row *DynamicRow
	return row.ResolveSchema(c.req.CustomTable)
}

func (c *ArtifactConversionCollector[S]) GetFromTime() *row_source.ResolvedFromTime {
	return c.source.GetFromTime()
}

func (c *ArtifactConversionCollector[S]) initialiseConfig(tableConfigData types.ConfigData) error {
	// default to empty config
	cfg := utils.InstanceOf[S]()

	if len(tableConfigData.GetHcl()) > 0 {
		var err error
		cfg, err = parse.ParseConfig[S](tableConfigData)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}

		slog.Info("tableName RowSourceImpl: config parsed", "config", c)
		c.Format = cfg
	}

	// validate config
	if err := c.Format.Validate(); err != nil {
		return fmt.Errorf("invalid partition config: %w", err)
	}

	return nil
}

func (c *ArtifactConversionCollector[S]) initialiseFormat(tableConfigData types.ConfigData) error {
	// default to empty config
	cfg := utils.InstanceOf[S]()

	if len(tableConfigData.GetHcl()) > 0 {
		var err error
		cfg, err = parse.ParseConfig[S](tableConfigData)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}

		slog.Info("tableName RowSourceImpl: config parsed", "config", c)
		c.Format = cfg
	}

	// validate config
	if err := c.Format.Validate(); err != nil {
		return fmt.Errorf("invalid partition config: %w", err)
	}

	return nil
}

// Collect executes the collection process. Tell our source to start collection
func (c *ArtifactConversionCollector[S]) Collect(ctx context.Context) (int, int, error) {
	// create empty status event#
	c.status = events.NewStatusEvent(c.req.ExecutionId)

	// tell our source to collect
	// this is a blocking call, but we will receive and process row events during the execution
	err := c.source.Collect(ctx)
	if err != nil {
		return 0, 0, err
	}

	slog.Info("Source collection complete - waiting for enrichment")

	// set the end time
	c.enrichTiming.End = time.Now()

	defer slog.Info("Enrichment complete")

	// notify observers of final status
	if err := c.NotifyObservers(ctx, c.status); err != nil {
		slog.Error("tableName RowSourceImpl: error notifying observers of status", "error", err)
	}

	// return the number of rows processed
	// TODO K
	//c.rowBufferLock.RLock()
	//defer c.rowBufferLock.RUnlock()
	//return c.rowCount, c.chunkCount, nil
	return 0, 0, nil
}

// Notify implements observable.Observer
// it handles all events which collectorFuncMap may receive (these will all come from the source)
func (c *ArtifactConversionCollector[S]) Notify(ctx context.Context, event events.Event) error {
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

func (c *ArtifactConversionCollector[S]) GetTiming() (types.TimingCollection, error) {
	res, err := c.source.GetTiming()
	if err != nil {
		return types.TimingCollection{}, err
	}

	return append(res, c.enrichTiming), nil
}

func (c *ArtifactConversionCollector[S]) initSource(ctx context.Context, configData *types.SourceConfigData, connectionData *types.ConnectionConfigData) error {
	requestedSource := configData.Type
	// must be an artifact source
	if !row_source.IsArtifactSource(requestedSource) {
		return fmt.Errorf("source type %s is not an artifact source", requestedSource)
	}

	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata := &SourceMetadata[*DynamicRow]{
		SourceName: requestedSource,
	}

	// TODO KAI FIX ME
	params := &row_source.RowSourceParams{
		SourceConfigData: configData,
		ConnectionData:   connectionData,
	}

	// ask factory to create and initialise the source for us
	// NOTE: we pass the original
	source, err := row_source.Factory.GetRowSource(ctx, params, sourceMetadata.Options...)
	if err != nil {
		return err
	}

	c.source = source

	// there must NOT be a mapper registered for the source - ArtifactConversionCollector does not support mappers
	if mapper := sourceMetadata.Mapper; mapper != nil {
		return errors.New("ArtifactConversionCollector does not support mappers")
	}

	// add ourselves as an observer to our Source
	return c.source.AddObserver(c)
}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (c *ArtifactConversionCollector[S]) updateStatus(ctx context.Context, e events.Event) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	c.status.Update(e)

	// send a status event periodically
	if time.Since(c.lastStatusEventTime) > statusUpdateInterval {
		// notify observers
		if err := c.NotifyObservers(ctx, c.status); err != nil {
			slog.Error("tableName RowSourceImpl: error notifying observers of status", "error", err)
		}
		// update lastStatusEventTime
		c.lastStatusEventTime = time.Now()
	}
}

func (c *ArtifactConversionCollector[S]) handleArtifactDownloaded(ctx context.Context, e *events.ArtifactDownloaded) error {
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

	//TODO K delete local artifact
	return nil

}

// OnChunk is called by the we have written a chunk of enriched rows to a [JSONL/CSV] file
// notify observers of the chunk
func (c *ArtifactConversionCollector[S]) OnChunk(ctx context.Context, chunkNumber int, collectionState json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// TODO #collectionstate SAVE collection state???
	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber)

	return c.NotifyObservers(ctx, e)
}
