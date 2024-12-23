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

// RegisterTableFormat registers a collector constructor for a table which supports Format
// this is called from the package init function of the table implementation
func RegisterTableFormat[R types.RowStruct, S parse.Config, T TableWithFormat[R, S]]() {
	collectorFunc := func() Collector {
		return NewCollectorWithFormat[R, S, T]()
	}

	Factory.registerCollector(collectorFunc)
}

// RegisterTable registers a collector constructor with the factory
// this is called from the package init function of the table implementation
func RegisterTable[R types.RowStruct, T Table[R]]() {
	collectorFunc := func() Collector {
		return &CollectorImpl[R]{
			Table: utils.InstanceOf[T](),
		}
	}

	Factory.registerCollector(collectorFunc)
}

// RegisterCollector registers a collector constructor directly
// this is only used if we need to specify a custom collector (used for custom tables)
func RegisterCollector(collectorFunc func() Collector) {
	Factory.registerCollector(collectorFunc)
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
	}
}

// registerCollector just store the constructor in an array
// This will be called before the Init function is called
// Init creates instances of each table - these ar eused to get the table identifier
// (for the map key) and the schema
// we defer this until TableFactory.Init as registerCollector is called from
// package init functions which cannot return an error
func (f *TableFactory) registerCollector(ctor func() Collector) {
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
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// populateSchemas builds the map of table constructors and schemas
// NOTE we could call this from Init but we only need it if a describe call is made so do it lazily
func (f *TableFactory) populateSchemas() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()

	// create schema map
	f.schemaMap = make(schema.SchemaMap)

	errs := make([]error, 0)

	for _, ctor := range f.collectorFuncs {
		// create an instance of the table to get the identifier
		collector := ctor()

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
	// get the registered collector constructor for the table
	ctor, ok := f.collectorFuncMap[req.TableName]
	if !ok {
		// this type is not registered
		return nil, fmt.Errorf("table not found: %s", req.TableName)
	}

	// create the partition
	partition := ctor()

	return partition, nil
}

func (f *TableFactory) GetPartitions() map[string]func() Collector {
	return f.collectorFuncMap
}

func (f *TableFactory) GetSchema() (schema.SchemaMap, error) {
	if f.schemaMap == nil {
		err := f.populateSchemas()
		if err != nil {
			return nil, err
		}
	}
	return f.schemaMap, nil
}

func (f *TableFactory) Initialized() bool {
	return len(f.collectorFuncMap) > 0
}

/*
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
var Factory TableFactory = newPluginTableFactory()

type TableFactory interface {
	RegisterCollector(collectorFunc func() Collector)
	Initialized() bool
	Init() error
	GetCollector(request *types.CollectRequest) (Collector, error)
	GetSchema() (schema.SchemaMap, error)

}


// RegisterTable registers a collector constructor with the factory
// this is called from the package init function of the table implementation
func RegisterTable[R types.RowStruct, S parse.Config, T Table[R]]() {
	collectorFunc := func() Collector {
		return &CollectorImpl[R, S]{
			Table:utils.InstanceOf[T](),
		}
	}

	Factory.RegisterCollector(collectorFunc)
}


type PluginTableFactory struct {
	collectorFuncs []func() Collector
	// maps of collector constructors, keyed by the name of the registered table types
	collectorFuncMap map[string]func() Collector
	// map of table schemas
	schemaMap schema.SchemaMap
}

func newPluginTableFactory() *PluginTableFactory {
	return &PluginTableFactory{
		collectorFuncMap: make(map[string]func() Collector),
	}
}

// RegisterCollector just store the constructor in an array
// This will be called before the Init function is called
// Init creates instances of each table - these ar eused to get the table identifier
// (for the map key) and the schema
// we defer this until PluginTableFactory.Init as registerCollector is called from
// package init functions which cannot return an error
func (f *PluginTableFactory) RegisterCollector(ctor func() Collector) {
	f.collectorFuncs = append(f.collectorFuncs, ctor)

}

// Init builds the map of table constructors and schemas
func (f *PluginTableFactory) Init() (err error) {
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
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// populateSchemas builds the map of table constructors and schemas
// NOTE we could call this from Init but we only need it if a describe call is made so do it lazily
func (f *PluginTableFactory) populateSchemas() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()

	// create schema map
	f.schemaMap = make(schema.SchemaMap)

	errs := make([]error, 0)

	for _, ctor := range f.collectorFuncs {
		// create an instance of the table to get the identifier
		collector := ctor()

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

func (f *PluginTableFactory) GetCollector(req *types.CollectRequest) (Collector, error) {
	// get the registered partition constructor for the table
	ctor, ok := f.collectorFuncMap[req.PartitionData.Table]
	if !ok {
		// this type is not registered
		return nil, fmt.Errorf("Table not found: %s", req.PartitionData.Table)
	}

	// create the partition
	partition := ctor()

	return partition, nil
}

func (f *PluginTableFactory) GetPartitions() map[string]func() Collector {
	return f.collectorFuncMap
}

func (f *PluginTableFactory) GetSchema() (schema.SchemaMap, error) {
	if f.schemaMap == nil {
		err := f.populateSchemas()
		if err != nil {
			return nil, err
		}
	}
	return f.schemaMap, nil
}

func (f *PluginTableFactory) Initialized() bool {
	return len(f.collectorFuncMap) > 0
}

*/
