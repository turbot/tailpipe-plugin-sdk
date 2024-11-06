package config_data

import "github.com/hashicorp/hcl/v2"

type ConnectionConfigData struct {
	*ConfigDataImpl
	Name string
}

func NewConnectionConfigData(hcl []byte, decRange hcl.Range, name string) *ConnectionConfigData {
	return &ConnectionConfigData{
		ConfigDataImpl: &ConfigDataImpl{
			Hcl:   hcl,
			Range: decRange,
		},
		Name: name,
	}
}
