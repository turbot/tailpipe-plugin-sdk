package table

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"log/slog"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type ArtifactConversionCollector[S parse.Config] struct {
	observable.ObservableImpl

	source row_source.RowSource

	tableName string
	// the table config
	Config S

	// wait group to wait for all rows to be processed
	// this is incremented each time we receive a row event and decremented when we have processed it
	status              *events.Status
	lastStatusEventTime time.Time
	statusLock          sync.RWMutex
	enrichTiming        types.Timing
	req                 *types.CollectRequest
}

func NewArtifactConversionCollector[S parse.Config](tableName string) *ArtifactConversionCollector[S] {
	return &ArtifactConversionCollector[S]{
		tableName: tableName,
	}
}

func (c *ArtifactConversionCollector[S]) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req
	// parse partition config
	if err := c.initialiseConfig(req.PartitionData); err != nil {
		return err
	}

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

	// get the schema from the common fields
	s, err := schema.SchemaFromStruct(enrichment.CommonFields{})
	if err != nil {
		return nil, err
	}
	// set mode to partial
	s.Mode = schema.ModePartial

	// does the config implement GetSchema()
	// NOTE: the config may be nil here as this is called both from collection and from the factory
	// (if a Describe call has been made)
	if !helpers.IsNil(c.Config) {
		if d, ok := any(c.Config).(parse.DynamicTableConfig); ok {
			// return s from config, if defined (NO
			configuredSchema := d.GetSchema()
			if configuredSchema != nil {
				// if we have a schema from the config, use it (but do not overwrite the schema from the common fields)
				s.DefaultTo(configuredSchema)
			}
		}
	}

	return s, nil

}

func (c *ArtifactConversionCollector[S]) initialiseConfig(tableConfigData config_data.ConfigData) error {
	// default to empty config
	// default to empty config
	cfg := utils.InstanceOf[S]()

	if len(tableConfigData.GetHcl()) > 0 {
		var err error
		cfg, err = parse.ParseConfig[S](tableConfigData)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}

		slog.Info("tableName RowSourceImpl: config parsed", "config", c)
		c.Config = cfg
	}

	// validate config
	if err := c.Config.Validate(); err != nil {
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

func (c *ArtifactConversionCollector[S]) GetTiming() types.TimingCollection {
	return append(c.source.GetTiming(), c.enrichTiming)
}

func (c *ArtifactConversionCollector[S]) initSource(ctx context.Context, configData *config_data.SourceConfigData, connectionData *config_data.ConnectionConfigData) error {
	requestedSource := configData.Type
	// must be an srtifact source
	if !row_source.IsArtifactSource(requestedSource) {
		return fmt.Errorf("source type %s is not an artifact source", requestedSource)
	}

	// get the source metadata for this source type
	// (this returns an error if the source is not supported by the table)
	sourceMetadata := &SourceMetadata[*DynamicRow]{
		SourceName: requestedSource,
	}
	// ask factory to create and initialise the source for us
	// NOTE: we pass the original
	source, err := row_source.Factory.GetRowSource(ctx, configData, connectionData, sourceMetadata.Options...)
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

	return nil

}

// OnChunk is called by the we have written a chunk of enriched rows to a [JSONL/CSV] file
// notify observers of the chunk
func (c *ArtifactConversionCollector[S]) OnChunk(ctx context.Context, chunkNumber int, collectionState json.RawMessage) error {
	executionId, err := context_values.ExecutionIdFromContext(ctx)
	if err != nil {
		return err
	}
	// construct proto event
	e := events.NewChunkEvent(executionId, chunkNumber, collectionState)

	return c.NotifyObservers(ctx, e)
}
