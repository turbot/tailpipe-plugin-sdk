package table

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"golang.org/x/exp/maps"
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
		return NewRowEnrichmentCollector[R](t)
	}
	Factory.registerCollector(t.Identifier(), collectorFunc)
}

func RegisterFormat[T formats.Format]() {
	f := utils.InstanceOf[T]()
	formatFunc := func() formats.Format { return f }
	Factory.registerFormat(f.Identifier(), formatFunc)
}
func RegisterFormatPresets(presets ...formats.Format) {
	Factory.registerFormatPresets(presets...)
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

// DescribeCustomFormats describes the custom formats which are provided in the request
func (f *TableFactory) DescribeCustomFormats(customFormatData []*proto.FormatData) (*types.DescribeResponse, error) {
	resp := &types.DescribeResponse{
		CustomFormats: make(types.FormatDescriptionMap),
	}

	// now parse custom formats
	customFormats, errs := f.parseCustomFormats(customFormatData)
	if len(errs) > 0 {
		errString := fmt.Sprintf("%d custom format parsing %s:\n", len(errs), utils.Pluralize("error", len(errs)))
		for formatName, err := range errs {
			errString += fmt.Sprintf("%s: %s\n", formatName, err.Error())
		}
		err := errors.New(errString)
		return nil, err
	}

	for _, format := range customFormats {
		formatDescription := f.describeFormat(format)
		// add the format to the map
		formatFullName := fmt.Sprintf("%s.%s", format.Identifier(), format.GetName())
		resp.CustomFormats[formatFullName] = formatDescription
	}

	return resp, nil
}

// DescribeFormats describes the formats available for the plugin, including provided custom formats
func (f *TableFactory) DescribeFormats(customFormatData ...*proto.FormatData) (*types.DescribeResponse, error) {
	// first describe the formats provided by the request
	resp, err := f.DescribeCustomFormats(customFormatData)
	if err != nil {
		return nil, err
	}

	// add format presets
	resp.FormatPresets = make(types.FormatDescriptionMap)
	for name, preset := range f.formatPresets {
		presetDescription := f.describeFormat(preset)
		resp.FormatPresets[name] = presetDescription

	}

	// add format types
	resp.FormatTypes = maps.Keys(f.formatMap)

	return resp, nil
}

func (f *TableFactory) describeFormat(format formats.Format) *types.FormatDescription {
	regex, err := format.GetRegex()
	if err != nil {
		regex = fmt.Sprintf("failed to convert pattern to regex: %s", err.Error())
	}

	return &types.FormatDescription{
		Type:        format.Identifier(),
		Name:        format.GetName(),
		Properties:  format.GetProperties(),
		Description: format.GetDescription(),
		Regex:       regex,
	}
}

// parseCustomFormats parses an array of custom formats - if any fail to parse we return a list of errors
// this function is used to parse custom formats provided in the config for the describe call - we will not fail on
// parse failure but instead want to show a message
func (f *TableFactory) parseCustomFormats(customFormatConfigs []*proto.FormatData) ([]formats.Format, map[string]error) {
	var res []formats.Format
	var errs = make(map[string]error)
	for _, formatConfig := range customFormatConfigs {
		// handle preset
		if formatConfig.PresetName != "" {
			if preset, ok := f.formatPresets[formatConfig.PresetName]; ok {
				res = append(res, preset)
			} else {
				errs[formatConfig.PresetName] = fmt.Errorf("preset format not found: %s", formatConfig.PresetName)
			}
			continue
		}
		if formatConfig.Regex != "" {
			// unexpected as normally a regex is only set when have already described a format
			errs[formatConfig.Regex] = fmt.Errorf("regex format cannot be used when describing custom formats")
			continue
		}
		formatData, err := types.FormatConfigDataFromProto(formatConfig)
		if err != nil {
			errs[formatConfig.Config.Target] = err
			continue
		}
		format, err := formats.ParseFormat(formatData, f.formatMap)
		if err != nil {
			errs[formatConfig.Config.Target] = err
			continue
		}
		res = append(res, format)
	}
	return res, errs
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

func (f *TableFactory) registerFormat(formatType string, formatFunc func() formats.Format) {
	// build the format full name (i.e. type.name)
	// this is used as the key in the format map
	f.formatMap[formatType] = formatFunc
}

func (f *TableFactory) registerFormatPresets(presets ...formats.Format) {
	for _, preset := range presets {
		presetFullName := fmt.Sprintf("%s.%s", preset.Identifier(), preset.GetName())
		f.formatPresets[presetFullName] = preset
	}
}

func (f *TableFactory) registerCustomTable(name string, ctor func() CustomTable) {
	f.customTableMap[name] = ctor
}

func (f *TableFactory) getCustomTableCollector(req *types.CollectRequest, customCtor func() CustomTable) (Collector, error) {
	customTable := customCtor()
	format, err := f.getFormatForTable(req, customTable)
	if err != nil {
		return nil, err
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
		return NewRowEnrichmentCollector[*types.DynamicRow](customTable), nil
	}
}

func (f *TableFactory) getFormatForTable(req *types.CollectRequest, customTable CustomTable) (formats.Format, error) {
	// if no format was provided use default
	if req.SourceFormat == nil {
		return customTable.GetDefaultFormat(), nil
	}
	// if a regex was provided, use it
	if req.SourceFormat.Regex != "" {
		return &formats.Regex{
			Layout: req.SourceFormat.Regex,
		}, nil
	}

	// if there is a preset, resolve it
	if req.SourceFormat.PresetName != "" {
		preset, ok := f.formatPresets[req.SourceFormat.PresetName]
		if !ok {
			return nil, fmt.Errorf("format preset not found: %s", req.SourceFormat.PresetName)
		}
		return preset, nil
	}

	// parse the format config
	return formats.ParseFormat(req.SourceFormat, f.formatMap)
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
		// for custom tables, we need to initialize the table  before we can get the schema

		tableDef := customTable.GetTableDefinition()
		// initialize the table
		customTable.Initialize(customTable.GetDefaultFormat(), tableDef)
		// now get the schema
		s, _ := customTable.GetSchema()
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
