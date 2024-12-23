package types

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type SourceConfigData struct {
	*ConfigDataImpl
	Type           string
	ReattachConfig *SourcePluginReattach
}

func (d SourceConfigData) SetReattach(pr *proto.SourcePluginReattach) {
	d.ReattachConfig = ReattachFromProto(pr)
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
