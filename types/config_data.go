package types

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"strings"
)

// ConfigData is a struct used to contain the config data used to configure a Collection or Source
// it contains the type of cource/collection, as well as the raw HCL config which the newly
// instantiated object must parse into the appropriate type
type ConfigData interface {
	GetHcl() []byte
	GetRange() hcl.Range
	Identifier() string
	ToProto() *proto.ConfigData
	GetConfigType() string
}

type ConfigDataImpl struct {
	Hcl   []byte
	Range hcl.Range
	// ConfigType is the type of the config data, i.e. connection, source, partition, format
	ConfigType string
	// InstanceType represent the subtype type of the config instance:
	// - if this is a partition config, this will be the table name
	// - if this is a source config, this will be the source type
	// - if this is a connection config, this will be the connection type (i.e. plugin name)
	// - if this is a format config, this will be the format type
	InstanceType string
}

// GetHcl returns the HCL config data
func (c *ConfigDataImpl) GetHcl() []byte {
	return c.Hcl
}

// GetRange returns the HCL range of the config data
func (c *ConfigDataImpl) GetRange() hcl.Range {
	return c.Range
}

// Identifier returns the identifier of the config data
func (c *ConfigDataImpl) Identifier() string {
	return c.InstanceType
}

// GetConfigType returns the type of the config data
func (c *ConfigDataImpl) GetConfigType() string {
	return c.ConfigType
}

func ConfigDataFromProto[T ConfigData](data *proto.ConfigData) (T, error) {
	/* the target field is the name ofd the target - this will be either:
	- a source: source.<source_type>
	- a partition: partition.<table>.<partition_name>
	- a connection: connection.<connection_name>
	*/

	var empty T
	parts := strings.Split(data.Target, ".")
	switch any(empty).(type) {
	case *SourceConfigData:
		if len(parts) != 2 {
			return empty, fmt.Errorf("invalid source config target %s: expected a name of form 'source.<type>'", data.Target)
		}
		if parts[0] != constants.ConfigTypeSource {
			return empty, fmt.Errorf("invalid source config target %s: expected a source", data.Target)
		}
		d := NewSourceConfigData(data.Hcl, proto.RangeFromProto(data.Range), parts[1])
		return ConfigData(d).(T), nil

	case *ConnectionConfigData:
		if len(parts) != 2 {
			return empty, fmt.Errorf("invalid connection config target %s: expected a form 'connection.<type>'", data.Target)
		}
		if parts[0] != constants.ConfigTypeConnection {
			return empty, fmt.Errorf("invalid connection config target %s: expected a connection", data.Target)
		}
		d := NewConnectionConfigData(data.Hcl, proto.RangeFromProto(data.Range), parts[1])
		return ConfigData(d).(T), nil
	case *FormatConfigData:
		if len(parts) != 2 {
			return empty, fmt.Errorf("invalid format config target %s: expected a name of form 'format.<type>'", data.Target)
		}
		if parts[0] != constants.ConfigTypeFormat {
			return empty, fmt.Errorf("invalid format config target %s: expected a connection", data.Target)
		}

		d := NewFormatConfigData(data.Hcl, proto.RangeFromProto(data.Range), parts[1])
		return ConfigData(d).(T), nil
	default:
		return empty, fmt.Errorf("invalid config type %T", empty)
	}
}

func (c *ConfigDataImpl) ToProto() *proto.ConfigData {
	return &proto.ConfigData{
		Hcl:    c.Hcl,
		Range:  proto.RangeToProto(c.Range),
		Target: fmt.Sprintf("%s.%s", c.ConfigType, c.InstanceType),
	}
}
