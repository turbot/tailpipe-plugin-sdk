package types

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

type ConnectionConfigData struct {
	*ConfigDataImpl
}

func NewConnectionConfigData(hcl []byte, decRange hcl.Range, ty string) *ConnectionConfigData {
	return &ConnectionConfigData{
		ConfigDataImpl: &ConfigDataImpl{
			Hcl:          hcl,
			Range:        decRange,
			InstanceType: ty,
			ConfigType:   constants.ConfigTypeConnection,
		},
	}
}
