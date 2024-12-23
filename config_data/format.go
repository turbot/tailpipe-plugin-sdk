package config_data

import "github.com/hashicorp/hcl/v2"

type FormatConfigData struct {
	*ConfigDataImpl
	Type string
}

func NewFormatConfigData(hcl []byte, decRange hcl.Range, formatType string) *FormatConfigData {
	return &FormatConfigData{
		ConfigDataImpl: &ConfigDataImpl{
			Hcl:        hcl,
			Range:      decRange,
			Id:         formatType,
			ConfigType: "format",
		},
		Type: formatType,
	}
}
