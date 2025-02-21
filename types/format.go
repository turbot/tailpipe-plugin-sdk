package types

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

type FormatConfigData struct {
	*ConfigDataImpl
}

func NewFormatConfigData(hcl []byte, decRange hcl.Range, formatType string) *FormatConfigData {
	return &FormatConfigData{
		ConfigDataImpl: &ConfigDataImpl{
			Hcl:          hcl,
			Range:        decRange,
			InstanceType: formatType,
			ConfigType:   constants.ConfigTypeFormat,
		},
	}
}
