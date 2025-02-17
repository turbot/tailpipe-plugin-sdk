package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

func ParseFormat(formatData types.ConfigData) (parse.Config, error) {
	// we expect the config to be format data
	if formatData.GetConfigType() != constants.ConfigTypeFormat {
		return nil, fmt.Errorf("invalid config type: expected format, got %s", formatData.GetConfigType())
	}

	// switch on the type of the config data subtype (Identifier)
	var format parse.Config
	var err error
	switch formatData.Identifier() {
	case constants.SourceFormatGrok:
		format, err = parse.ParseConfig[*Grok](formatData)
		if err != nil {
			return nil, fmt.Errorf("error parsing config: %w", err)
		}

	case constants.SourceFormatRegex:
		format, err = parse.ParseConfig[*Regex](formatData)
		if err != nil {
			return nil, fmt.Errorf("error parsing config: %w", err)
		}

	case constants.SourceFormatDelimited:
		format, err = parse.ParseConfig[*Delimited](formatData)
		if err != nil {
			return nil, fmt.Errorf("error parsing config: %w", err)
		}
	//case constants.SourceFormatJson:
	//case constants.SourceFormatJsonLines:
	default:
		return nil, fmt.Errorf("format not yet implemented: %s", formatData.Identifier())

	}

	err = format.Validate()
	if err != nil {
		return nil, fmt.Errorf("error validating format: %w", err)
	}
	return format, nil
}
