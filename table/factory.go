package table

import (
	"errors"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Factory is a global TableFactory instance
var Factory = newTableFactory()

// RegisterCustomTable registers a collector constructor for a table which supports Formatr
// this is called from the package init function of the table implementation
func RegisterCustomTable[T CustomTable](opts ...CustomTableOpt) {
	var collectorFunc func() Collector

	// create table instance
	t := utils.InstanceOf[T]()

	//There are 2 uses cases for custom tables:
	//- fully custom tables implements by the LogTable in the core plugin
	//- predefined custom tables which may be implemented by any plugin and have a fixed format and table definition

	// In the case of fully custom tables, the format and table definition are defined in config.
	// The opts passed to this function will include WithTableDef, whach sets the format and table def for the table
	// by calling t.Initialize(format, tableDef)

	// apply any options to the table
	// this is used to set the format and table def for fully custom tables
	for _, opt := range opts {
		opt(t)
	}

	// In the case of predefined custom tables, the format and table def are defined in the table implementation,
	// and returned by the interface functions GetFormat and GetTableDef.
	// For this usage wqe need to populate the format and table def of the embedded CustomTableImpl struct
	// by calling Initialize
	// (this does mean that for custom tables we call Initialize twice, but it is a cheap call)
	t.Initialize(t.GetFormat(), t.GetTableDef())

	f := t.GetFormat()
	switch f.(type) {
	case *formats.Grok, *formats.Regex:
		collectorFunc = func() Collector {
			return &CollectorImpl[*DynamicRow]{Table: t}
		}
	case *formats.Delimited:
		collectorFunc = func() Collector {
			// TODO
			return NewArtifactConversionCollector()
		}
	}

	// now register the collector
	Factory.registerCollector(t.Identifier(), collectorFunc)
}

// RegisterTable registers a collector constructor with the factory
// this is called from the package init function of the table implementation
func RegisterTable[R types.RowStruct, T Table[R]]() {
	t := utils.InstanceOf[T]()
	collectorFunc := func() Collector {
		return &CollectorImpl[R]{
			Table: t,
		}
	}

	Factory.registerCollector(t.Identifier(), collectorFunc)
}

type TableFactory struct {
	// maps of collector constructors, keyed by the name of the table name
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
func (f *TableFactory) registerCollector(name string, ctor func() Collector) {
	f.collectorFuncMap[name] = ctor
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

	for _, ctor := range f.collectorFuncMap {
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
	collector := ctor()

	return collector, nil
}

func (f *TableFactory) GetCollectorMap() map[string]func() Collector {
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
