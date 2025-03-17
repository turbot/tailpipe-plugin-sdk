package types

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type FormatConfigData struct {
	*ConfigDataImpl
	Name           string
	ReattachConfig *SourcePluginReattach
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

func (d *FormatConfigData) SetReattach(pr *proto.SourcePluginReattach) {
	if pr == nil {
		return
	}
	d.ReattachConfig = ReattachFromProto(pr)
}

func FormatConfigDataFromProto(data *proto.FormatData) (*FormatConfigData, error) {
	configData, err := ConfigDataFromProto[*FormatConfigData](data.Config)
	if err != nil {
		return nil, fmt.Errorf("error parsing format config data: %w", err)
	}
	configData.Name = data.Name
	return configData, nil
}
