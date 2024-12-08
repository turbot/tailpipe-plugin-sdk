package table

import (
	"errors"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Factory is a global TableFactory instance
var Factory = newTableFactory()

// RegisterTable registers a table constructor with the factory
// this is called from the package init function of the table implementation
func RegisterTable[R types.RowStruct, S parse.Config, T Table[R, S]]() {
	tableFunc := func() Collector {
		return &CollectorImpl[R, S, T]{
			table: utils.InstanceOf[T](),
		}
	}

	Factory.registerTable(tableFunc)
}

type TableFactory struct {
	collectorFuncs []func() Collector
	// maps of collector constructors, keyed by the name of the registered table types
	collectorFuncMap map[string]func() Collector
	// map of table schemas
	schemaMap schema.SchemaMap
}

func newTableFactory() TableFactory {
	return TableFactory{
		collectorFuncMap: make(map[string]func() Collector),
		schemaMap:        make(schema.SchemaMap),
	}
}

// registerTable just store the constructor in an array
// This will be called before the Init function is called
// Init creates instances of each table - these ar eused to get the table identifier
// (for the map key) and the schema
// we defer this until TableFactory.Init as registerTable is called from
// package init functions which cannot return an error
func (f *TableFactory) registerTable(ctor func() Collector) {
	f.collectorFuncs = append(f.collectorFuncs, ctor)

}

// Init builds the map of table constructors and schemas
func (f *TableFactory) Init() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()

	errs := make([]error, 0)

	for _, ctor := range f.collectorFuncs {
		// create an instance of the table to get the identifier
		collector := ctor()

		// register the collector func with the table factory
		f.collectorFuncMap[collector.Identifier()] = ctor

		// get the schema for the table row type
		s, err := collector.GetSchema()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// merge in the common schema
		f.schemaMap[collector.Identifier()] = s
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (f *TableFactory) GetCollector(req *types.CollectRequest) (Collector, error) {
	// get the registered partition constructor for the table
	ctor, ok := f.collectorFuncMap[req.PartitionData.Table]
	if !ok {
		// this type is not registered
		return nil, fmt.Errorf("table not found: %s", req.PartitionData.Table)
	}

	// create the partition
	partition := ctor()

	return partition, nil
}

func (f *TableFactory) GetPartitions() map[string]func() Collector {
	return f.collectorFuncMap
}

func (f *TableFactory) GetSchema() schema.SchemaMap {
	return f.schemaMap
}

func (f *TableFactory) Initialized() bool {
	return len(f.collectorFuncMap) > 0
}
