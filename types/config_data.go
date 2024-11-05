package types

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

// ConfigData is a struct used to contain the config data used to configure a Collection or Source
// it contains the type of cource/collection, as well as the raw HCL config which the newly
// instantiated object must parse into the appropriate type
type ConfigData struct {
	// the type of the config target (coolection.source)
	Type  string
	Hcl   []byte
	Range hcl.Range
}

func DataFromProto(data *proto.ConfigData) *ConfigData {
	if data == nil {
		return nil
	}
	return &ConfigData{
		Type:  data.Type,
		Hcl:   data.Hcl,
		Range: proto.RangeFromProto(data.Range),
	}
}
