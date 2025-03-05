package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

func ParseFormat(formatData types.ConfigData, formatConstructorMap map[string]func() Format) (Format, error) {
	// we expect the config to be format data
	if formatData.GetConfigType() != constants.ConfigTypeFormat {
		return nil, fmt.Errorf("invalid config type: expected format, got %s", formatData.GetConfigType())
	}
	// is this format type supported
	formatCtor, isSupported := formatConstructorMap[formatData.Identifier()]
	if !isSupported {
		return nil, fmt.Errorf("unsupported format: %s", formatData.Identifier())
	}

	format := formatCtor()
	err := parse.ParseConfigIntoTarget(formatData, format)
	if err != nil {
		return nil, fmt.Errorf("error parsing format data: %w", err)
	}

	err = format.Validate()
	if err != nil {
		return nil, fmt.Errorf("error validating format: %w", err)
	}
	return format, nil
}
