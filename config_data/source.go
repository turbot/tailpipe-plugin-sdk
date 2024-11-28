package config_data

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type SourceConfigData struct {
	*ConfigDataImpl
	Type           string
	ReattachConfig *types.SourcePluginReattach
}

func (d SourceConfigData) SetReattach(pr *proto.ReattachConfig) {
	d.ReattachConfig = types.ReattachFromProto(pr)
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
