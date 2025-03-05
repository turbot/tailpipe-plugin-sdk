package table

import (
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"golang.org/x/exp/maps"
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

func RegisterFormat[T formats.Format](presets ...T) {
	f := utils.InstanceOf[T]()

	formatFunc := func() formats.Format { return f }
	// convert the presets to a slice of formats.Format
	presetIfs := make([]formats.Format, len(presets))
	for i, preset := range presets {
		presetIfs[i] = preset
	}

	Factory.registerFormat(f.Identifier(), formatFunc, presetIfs)
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

	// a map of formats provided by plugin
	formatMap     map[string]func() formats.Format
	formatPresets map[string]formats.Format

	// map of table schemas
	schemaMap schema.SchemaMap
}

func newTableFactory() TableFactory {
	return TableFactory{
		collectorFuncMap: make(map[string]func() Collector),
		customTableMap:   make(map[string]func() CustomTable),
		formatMap:        make(map[string]func() formats.Format),
		formatPresets:    make(map[string]formats.Format),
	}
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
func (f *TableFactory) DescribeFormats(customFormatConfigs []*proto.ConfigData) (presetDescriptions, customFormatDescriptions formats.FormatDescriptionMap, formatTypes []string, err error) {
	presetDescriptions = make(formats.FormatDescriptionMap)
	customFormatDescriptions = make(formats.FormatDescriptionMap)

	// Add format presets
	for name, preset := range f.formatPresets {
		presetDescription := f.describeFormat(preset)
		presetDescriptions[name] = presetDescription

	}
	// now parse custom formats adnd add them to the map (they take precedence
	customFormats, errMap := f.parseCustomFormats(customFormatConfigs)

	for _, format := range customFormats {
		formatDescription := f.describeFormat(format)
		// add the format to the map
		formatFullName := fmt.Sprintf("%s.%s", format.Identifier(), format.GetName())

		customFormatDescriptions[formatFullName] = formatDescription
	}
	// now add the errors for any formats that failed to parse
	for formatType, typeMap := range errMap {
		for formatName, errMsg := range typeMap {
			fullName := fmt.Sprintf("%s.%s", formatType, formatName)
			customFormatDescriptions[fullName] = &formats.FormatDescription{
				Type:        formatType,
				Name:        formatName,
				Description: errMsg,
			}
		}
	}
	formatTypes = maps.Keys(f.formatMap)

	return presetDescriptions, customFormatDescriptions, formatTypes, nil
}

func (f *TableFactory) describeFormat(format formats.Format) *formats.FormatDescription {
	regex, err := format.GetRegex()
	if err != nil {
		regex = fmt.Sprintf("failed to convert pattern to regex: %s", err.Error())
	}

	return &formats.FormatDescription{
		Type:        format.Identifier(),
		Name:        format.GetName(),
		Properties:  format.GetProperties(),
		Description: format.GetDescription(),
		Regex:       regex,
	}
}

// parseCustomFormats parses an array of custom formats - if any fail to parse we rteturn a map of format name to error message
// this function is used to parse custom formats provided in the config for the describe call - we will not fail on
// parse failure but instead want to show a message
func (f *TableFactory) parseCustomFormats(customFormatConfigs []*proto.ConfigData) ([]formats.Format, map[string]map[string]string) {
	parseFailures := make(map[string]map[string]string)
	var res []formats.Format
	for _, formatConfig := range customFormatConfigs {
		formatData, err := types.ConfigDataFromProto[*types.FormatConfigData](formatConfig)
		if err != nil {
			// we do not know the format name so just put it into the map with type
			parseFailures[formatData.Identifier()]["unknown"] = err.Error()
			continue
		}
		format, err := f.GetFormat(formatData)
		if err != nil {
			// we do not know the format name so just put it into the map with type
			parseFailures[formatData.Identifier()][format.GetName()] = err.Error()
			continue
		}
		res = append(res, format)
	}
	return res, nil
}

// GetFormat returns a format instance for the given format config
func (f *TableFactory) GetFormat(formatConfig *types.FormatConfigData) (formats.Format, error) {
	// Try to parse the format using the map of registered format ctors
	return formats.ParseFormat(formatConfig, f.formatMap)
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

func (f *TableFactory) registerFormat(formatType string, formatFunc func() formats.Format, presets []formats.Format) {
	// build the format full name (i.e. type.name)
	// this is used as the key in the format map
	f.formatMap[formatType] = formatFunc
	for _, preset := range presets {
		presetFullName := fmt.Sprintf("%s.%s", formatType, preset.GetName())
		f.formatPresets[presetFullName] = preset
	}

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
		// for custom tables, we need to initialize the table with the table def and format before we can get the schem
		// get the defaults
		format := customTable.GetSupportedFormats().DefaultFormat
		tableDef := customTable.GetTableDefinition()
		// initialize the table
		customTable.Initialize(format, tableDef)
		// now get the schema
		s := customTable.GetSchema()
		// only add the schema if it is not nil (which would not be expected - as we should at least have the common row schema)
		if s != nil {
			f.schemaMap[customTable.Identifier()] = s
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (f *TableFactory) getCustomTableCollector(req *types.CollectRequest, customCtor func() CustomTable) (Collector, error) {
	customTable := customCtor()
	supportedFormats := customTable.GetSupportedFormats()
	// if no format was provided, use the default format
	format := supportedFormats.DefaultFormat

	// if a format was provided, parse it
	if req.SourceFormat != nil {
		var err error
		format, err = formats.ParseFormat(req.SourceFormat, supportedFormats.Formats)
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
