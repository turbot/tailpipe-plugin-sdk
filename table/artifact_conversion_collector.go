package table

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
)

// ArtifactConversionCollector is a collector that converts artifacts directly to JSONL
// S is the table config type
type ArtifactConversionCollector struct {
	CollectorImpl[*types.DynamicRow]

	tableName string
	// the source format
	//formatData *proto.ConfigData
	// the table config
	Format parse.Config

	req *types.CollectRequest
}

func NewArtifactConversionCollector(Table[*types.DynamicRow]) *ArtifactConversionCollector {
	return &ArtifactConversionCollector{
		// TODO
		//tableName:  tableDef.Name,
		//formatData: formatData,
	}
}

func (c *ArtifactConversionCollector) Init(ctx context.Context, req *types.CollectRequest) error {
	c.req = req

	// TODO #validate validate no extractor
	// TODO #validate validate table name does not clash

	slog.Info("tableName RowSourceImpl: Collect", "table", c.tableName)
	if err := c.initSource(ctx, req); err != nil {
		return err
	}
	slog.Info("Start collection")

	return nil
}

func (c *ArtifactConversionCollector) Identifier() string {
	return c.tableName
}

// GetSchema returns the schema of the table if available
// for dynamic tables, the schema is only available at this if the config contains a schema
func (c *ArtifactConversionCollector) GetSchema() (*schema.TableSchema, error) {
	return c.req.CustomTableSchema, nil
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

	//TODO K delete local artifact
	return nil

}
