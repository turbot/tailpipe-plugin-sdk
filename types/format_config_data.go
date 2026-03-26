package types

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// FormatConfigData is an sdk type which is mapped from the proto.FormatData
type FormatConfigData struct {
	*ConfigDataImpl
	Name       string
	PresetName string
	Regex      string
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

func (d *FormatConfigData) FullName() string {
	return d.InstanceType + "." + d.Name
}

func FormatConfigDataFromProto(fd *proto.FormatData) (*FormatConfigData, error) {
	// one of PresetName, Regex or Config will be set
	if fd.PresetName != "" {
		return &FormatConfigData{
			PresetName: fd.PresetName,
		}, nil
	}
	if fd.Regex != "" {
		return &FormatConfigData{
			Regex: fd.Regex,
		}, nil
	}

	configData, err := ConfigDataFromProto[*FormatConfigData](fd.Config)
	if err != nil {
		return nil, fmt.Errorf("error parsing format config fd: %w", err)
	}
	configData.Name = fd.Name
	return configData, nil
}
