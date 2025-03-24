package table

import (
	"context"
	"log/slog"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/error_types"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// how often to send status events

const statusUpdateInterval = 250 * time.Millisecond

// CollectorImpl is a generic implementation of the Collector interface
// it is responsible for coordinating the collection process and reporting status
// R is the type of the row struct
// S is the type of the partition config
// T is the type of the table
// U is the type of the connection
type CollectorImpl[R types.RowStruct] struct {
	observable.ObservableImpl

	Table  Table[R]
	source row_source.RowSource
	mapper mappers.Mapper[R]

	// wait group to wait for all rows to be processed
	// this is incremented each time we receive a row event and decremented when we have processed it
	status              *events.Status
	lastStatusEventTime time.Time

	req *types.CollectRequest
}

func (c *CollectorImpl[R]) Identifier() string {
	return c.Table.Identifier()
}

// GetFromTime returns the 'resolved' from time of the source
func (c *CollectorImpl[R]) GetFromTime() *row_source.ResolvedFromTime {
	return c.source.GetFromTime()
}

func (c *CollectorImpl[R]) initSource(ctx context.Context, req *types.CollectRequest, sourceMetadata *SourceMetadata[R]) error {

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

	// set mapper if source metadata specifies one
	if mapper := sourceMetadata.Mapper; mapper != nil {
		c.mapper = mapper
	}

	// add ourselves as an observer to our Source
	return c.source.AddObserver(c)
}

// ask table for it;s supported sources and put into map for ease of lookup
func (c *CollectorImpl[R]) getSourceMetadataMap() (map[string]*SourceMetadata[R], error) {
	supportedSources, err := c.Table.GetSourceMetadata()
	if err != nil {
		return nil, err
	}
	// convert to a map for easy lookup
	sourceMap := make(map[string]*SourceMetadata[R])
	for _, s := range supportedSources {
		sourceMap[s.SourceName] = s
	}
	return sourceMap, nil
}

// updateStatus updates the status counters with the latest event
// it also sends raises status event periodically (determined by statusUpdateInterval)
// note: we will send a final status event when the collection completes
func (c *CollectorImpl[R]) updateStatus(ctx context.Context, e events.Event) {
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

func (c *CollectorImpl[R]) Notify(ctx context.Context, event events.Event) error {
	return nil
}
