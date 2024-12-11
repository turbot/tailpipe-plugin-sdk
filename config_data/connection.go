package config_data

import "github.com/hashicorp/hcl/v2"

type ConnectionConfigData struct {
	*ConfigDataImpl
	Type string
}

func NewConnectionConfigData(hcl []byte, decRange hcl.Range, ty string) *ConnectionConfigData {
	return &ConnectionConfigData{
		ConfigDataImpl: &ConfigDataImpl{
			Hcl:        hcl,
			Range:      decRange,
			Id:         ty,
			ConfigType: "connection",
		},
		Type: ty,
	}
}
