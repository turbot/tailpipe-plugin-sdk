package table

import (
	"context"
	"errors"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Factory is a global TableFactory instance
var Factory = newTableFactory()

// RegisterTable registers a table constructor with the factory
// this is called from the package init function of the table implementation
func RegisterTable[R types.RowStruct, T Table[R]]() {
	tableFunc := func() TableCore {
		return utils.InstanceOf[T]()
	}
	Factory.registerTable(tableFunc)
}

type TableFactory struct {
	tableFuncs []func() TableCore
	// maps of constructors for  the various registered types
	tableFuncMap map[string]func() TableCore
	// map of table schemas
	schemaMap schema.SchemaMap
}

func newTableFactory() TableFactory {
	return TableFactory{
		tableFuncMap: make(map[string]func() TableCore),
		schemaMap:    make(schema.SchemaMap),
	}
}

// registerTable just store the constructor in an array
// This will be called before the Init function is called
// Init creates instances of each table - these ar eused to get the table identifier
// (for the map key) and the schema
// we defer this until TableFactory.Init as registerTable is called from
// package init functions which cannot return an error
func (f *TableFactory) registerTable(ctor func() TableCore) {
	f.tableFuncs = append(f.tableFuncs, ctor)

}

// Init builds the map of table constructors and schemas
func (f *TableFactory) Init() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()

	errs := make([]error, 0)

	for _, ctor := range f.tableFuncs {
		// create an instance of the table to get the identifier
		c := ctor()

		// register the table
		tableName := c.Identifier()
		f.tableFuncMap[tableName] = ctor

		// get the schema for the table row type
		rowStruct := c.GetRowSchema()
		s, err := schema.SchemaFromStruct(rowStruct)
		if err != nil {
			errs = append(errs, err)
		}
		// merge in the common schema
		f.schemaMap[tableName] = s
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (f *TableFactory) GetSchema() schema.SchemaMap {
	return f.schemaMap
}

func (f *TableFactory) GetTable(ctx context.Context, req *types.CollectRequest, connectionSchemaProvider ConnectionSchemaProvider) (TableCore, error) {
	// get the registered constructor for the table
	ctor, ok := f.tableFuncMap[req.PartitionData.Table]
	if !ok {
		// this type is not registered
		return nil, fmt.Errorf("table not found: %s", req.PartitionData.Table)
	}

	// create the table
	table := ctor()

	//  register the table implementation with the base struct (_before_ calling Init)

	base, ok := table.(baseTable)
	if !ok {
		return nil, fmt.Errorf("table implementation must embed table.TableImpl")
	}
	err := base.RegisterImpl(table)
	if err != nil {
		return nil, fmt.Errorf("failed to register table implementation: %w", err)
	}

	err = table.Init(ctx, connectionSchemaProvider, req)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise table: %w", err)
	}

	return table, nil
}

func (f *TableFactory) GetTables() map[string]func() TableCore {
	return f.tableFuncMap
}
