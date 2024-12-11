package config_data

import "github.com/hashicorp/hcl/v2"

type SourceConfigData struct {
	*ConfigDataImpl
	Type string
}

func NewSourceConfigData(hcl []byte, decRange hcl.Range, sourceType string) *SourceConfigData {
	return &SourceConfigData{
		ConfigDataImpl: &ConfigDataImpl{
			Hcl:        hcl,
			Range:      decRange,
			Id:         sourceType,
			ConfigType: "source",
		},
		Type: sourceType,
	}
}
