package table

import (
	"context"
	"errors"
	"fmt"

	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// Factory is a global TableFactory instance
var Factory = newTableFactory()

type TableFactory struct {
	// maps of constructors for  the various registered types
	tableFuncs map[string]func() Table
	// map of table schemas
	schemaMap schema.SchemaMap
}

func newTableFactory() TableFactory {
	return TableFactory{
		tableFuncs: make(map[string]func() Table),
		schemaMap:  make(schema.SchemaMap),
	}
}

func (f *TableFactory) RegisterTables(tableFuncs ...func() Table) error {
	errs := make([]error, 0)
	for _, ctor := range tableFuncs {
		// create an instance of the table to get the identifier
		c := ctor()
		// register the table
		f.tableFuncs[c.Identifier()] = ctor

		// get the schema for the table row type
		rowStruct := c.GetRowSchema()
		s, err := schema.SchemaFromStruct(rowStruct)
		if err != nil {
			errs = append(errs, err)
		}
		// merge in the common schema
		f.schemaMap[c.Identifier()] = s
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (f *TableFactory) GetSchema() schema.SchemaMap {
	return f.schemaMap
}

func (f *TableFactory) GetTable(ctx context.Context, req *proto.CollectRequest, connectionSchemaProvider ConnectionSchemaProvider) (Table, error) {
	// get the registered constructor for the table
	ctor, ok := f.tableFuncs[req.TableData.Type]
	if !ok {
		// this type is not registered
		return nil, fmt.Errorf("table not found: %s", req.TableData.Type)
	}

	// create the table
	table := ctor()

	//  register the table implementation with the base struct (_before_ calling Init)
	// create an interface type to use - we do not want to expose this function in the Table interface
	type baseTable interface{ RegisterImpl(Table) }

	base, ok := table.(baseTable)
	if !ok {
		return nil, fmt.Errorf("table implementation must embed table.TableBase")
	}
	base.RegisterImpl(table)

	// prepare the data needed for Init

	// convert req into tableConfigData and sourceConfigData
	tableConfigData := parse.DataFromProto(req.TableData)
	sourceConfigData := parse.DataFromProto(req.SourceData)
	connectionData := parse.DataFromProto(req.ConnectionData)

	err := table.Init(ctx, connectionSchemaProvider, tableConfigData, sourceConfigData, connectionData, req.CollectionState)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise table: %w", err)
	}

	return table, nil
}

func (f *TableFactory) GetTables() map[string]func() Table {
	return f.tableFuncs
}
