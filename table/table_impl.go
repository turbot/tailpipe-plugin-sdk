package table

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// TableImpl provides a base implementation of the [table.Table] interface
// it should be embedded in all Table implementations
// R is the type of the row struct
// S is the type table config struct
// T is the type of the connection
type TableImpl[R types.RowStruct, S, T parse.Config] struct {
	// store a reference to the actual table (via the generic Table interface) so we can call its methods
	table Table[R]

	// the table config
	Config S
}

// Init implements table.Table
func (b *TableImpl[R, S, T]) Init(ctx context.Context, req *types.CollectRequest) error {
	if err := b.initialiseConfig(req.PartitionData); err != nil {
		return err
	}

	// TODO K check this can be removes - do we need to add collection state to the source?
	//// initialise the source
	//sourceOpts := b.table.GetSourceOptions(req.SourceData.Type)
	//// if collectionStateJSON is non-empty, add an option to set it
	//if len(req.CollectionState) > 0 {
	//	sourceOpts = append(sourceOpts, row_source.WithCollectionStateJSON(req.CollectionState))
	//}
	//
	//if err := b.initSource(ctx, req.SourceData, sourceOpts...); err != nil {
	//	return err
	//}

	return nil
}

func (b *TableImpl[R, S, T]) initialiseConfig(tableConfigData config_data.ConfigData) error {
	if len(tableConfigData.GetHcl()) > 0 {
		c, err := parse.ParseConfig[S](tableConfigData)
		if err != nil {
			return fmt.Errorf("error parsing config: %w", err)
		}
		b.Config = c

		slog.Info("Table RowSourceImpl: config parsed", "config", c)

		// validate config
		if err := c.Validate(); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}
	}
	return nil
}

// RegisterImpl is called by the plugin implementation to register the collection implementation
// it also resisters the supported sources for this collection
// this is required so that the TableImpl can call the collection's methods
func (b *TableImpl[R, S, T]) RegisterImpl(impl TableCore) error {
	// we expect impl to be a Table[R]
	enricher, ok := impl.(Table[R])
	if !ok {
		// this is unexpected as we have already validated this when registering the table
		return fmt.Errorf("table %s does not implement Table", impl.Identifier())
	}
	b.table = enricher
	return nil
}

// GetRowSchema returns an empty instance of the row struct returned by the collection
func (b *TableImpl[R, S, T]) GetRowSchema() types.RowStruct {
	return utils.InstanceOf[R]()
}

// GetCollector returns the collector of the correct generic type
func (b *TableImpl[R, S, T]) GetCollector() Collector {
	return &RowCollector[R, S]{
		table: b.table,
	}
}
