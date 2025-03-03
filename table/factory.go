package table

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Factory is a global TableFactory instance
var Factory = newTableFactory()

type TableOption func(*tableConfig)

type tableConfig struct {
	name string
}

func WithName(name string) TableOption {
	return func(tc *tableConfig) {
		tc.name = name
	}
}

// RegisterCustomTable registers a custom table type with optional configuration
func RegisterCustomTable[T CustomTable](opts ...TableOption) {
	t := utils.InstanceOf[T]()
	cfg := &tableConfig{
		// default to type name
		name: t.Identifier(),
	}

	// Apply any options
	for _, opt := range opts {
		opt(cfg)
	}

	customTableFunc := func() CustomTable { return t }
	Factory.registerCustomTable(cfg.name, customTableFunc)
}

// RegisterTable registers a collector constructor with the factory
// this is called from the package init function of the table implementation
func RegisterTable[R types.RowStruct, T Table[R]]() {
	t := utils.InstanceOf[T]()
	collectorFunc := func() Collector {
		return NewCollectorImpl[R](t)
	}
	Factory.registerCollector(t.Identifier(), collectorFunc)
}

type TableFactory struct {
	// maps of collector constructors, keyed by the name of the table name
	// these are registered for static tables
	// NOTE: we store the collector ctor not the table ctor as tables are generic so with different row types
	// so cannot be stored in a map
	collectorFuncMap map[string]func() Collector

	// custom tables registered with the factory
	// these are registered with a table constructor function
	// NOTE: we store the table not the collector ctor as the collector type may be different for each table
	// (could still do the logic in the ctor)
	customTableMap map[string]func() CustomTable

	// map of table schemas
	schemaMap schema.SchemaMap
}

func newTableFactory() TableFactory {
	return TableFactory{
		collectorFuncMap: make(map[string]func() Collector),
		customTableMap:   make(map[string]func() CustomTable),
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

func (f *TableFactory) registerCustomTable(name string, ctor func() CustomTable) {
	f.customTableMap[name] = ctor
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
		f.schemaMap[collector.Identifier()] = s
	}
	// now do the custom tables - only predefined custom tables will have schema
	for _, ctor := range f.customTableMap {
		// create an instance of the table to get the identifier
		customTable := ctor()

		// get the schema for the table row type
		s := customTable.GetSchema()
		if s != nil {
			f.schemaMap[customTable.Identifier()] = s
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (f *TableFactory) GetCollector(req *types.CollectRequest) (Collector, error) {
	// get the registered collector constructor for the table
	// is it a static table (i.e. in the collectorFuncMap) or a custom table
	collectorCtor, ok := f.collectorFuncMap[req.TableName]
	if ok {
		// create the collector
		return collectorCtor(), nil
	}

	// check for custom table
	if customTableCtor, ok := f.customTableMap[req.TableName]; ok {
		return f.getCustomTableCollector(req, customTableCtor)
	}
	// this type is not registered
	return nil, fmt.Errorf("table not found: %s", req.TableName)
}

func (f *TableFactory) getCustomTableCollector(req *types.CollectRequest, customCtor func() CustomTable) (Collector, error) {
	customTable := customCtor()
	supportedFormats := customTable.GetSupportedFormats()
	// if no format was provided, use the default format
	format := supportedFormats.DefaultFormat

	// if a format was provided, parse it
	if req.SourceFormat != nil {
		var err error
		format, err = formats.ParseFormat(req.SourceFormat, supportedFormats)
		if err != nil {
			slog.Warn("error parsing format", "error", err)
			return nil, err
		}
	}

	// if we now do not have a format, return an error
	if format == nil {
		slog.Warn("no supported format provided in config and table does not define a default format", "table", req.TableName)
		return nil, fmt.Errorf("no supported format found for table %s", req.TableName)
	}
	// the table may provide a table definition - default to this
	tableDef := customTable.GetTableDefinition()

	// if a table definition was provided in the req, use it
	if req.CustomTableSchema != nil {
		tableDef = req.CustomTableSchema
	}
	// now initialize the custom table with the format and table definition
	customTable.Initialize(format, tableDef)

	// now create the appropriate type of collector
	switch format.Identifier() {
	case constants.SourceFormatDelimited, constants.SourceFormatJson, constants.SourceFormatJsonLines:
		return NewArtifactConversionCollector(customTable), nil
	default:
		return NewCollectorImpl[*types.DynamicRow](customTable), nil
	}
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

// DescribeFormats returns a map of format instances -
func (f *TableFactory) DescribeFormats() formats.FormatMap {
	res := make(formats.FormatMap)
	for _, customTable := range f.customTableMap {
		supportedFormats := customTable().GetSupportedFormats()

		// if no formats are supported, skip
		if supportedFormats == nil || len(supportedFormats.FormatInstances) == 0 {
			continue
		}

		for _, format := range supportedFormats.FormatInstances {
			formatType := format.Identifier()
			formatsForType := res[formatType]

			regex, err := format.GetRegex()
			if err != nil {
				regex = fmt.Sprintf("faild to convert pattern to regex: %s", err.Error())
			}

			formatsForType = append(formatsForType, &formats.FormatDescription{
				Type:        format.Identifier(),
				Name:        format.GetName(),
				FormatString: format.GetFormatString(),
				Description: format.GetDescription(),
				Regex:       regex,
			})
			res[formatType] = formatsForType
		}
	}
	return res
}