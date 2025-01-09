package proto

import (
	"github.com/hashicorp/go-plugin"
)

func NewSourcePluginReattach(sourceType, pluginName string, src *plugin.ReattachConfig) *SourcePluginReattach {
	return &SourcePluginReattach{
		ReattachConfig: &ReattachConfig{
			Protocol:        string(src.Protocol),
			ProtocolVersion: int64(src.ProtocolVersion),
			Addr: &NetAddr{
				Network: src.Addr.Network(),
				Address: src.Addr.String(),
			},
			Pid: int64(src.Pid),
		},
		SourceType: sourceType,
		Plugin:     pluginName,
	}
}

// ToPluginReattach converts from a protobuf reattach config to a plugin.ReattachConfig
func (r *ReattachConfig) ToPluginReattach() *plugin.ReattachConfig {
	return &plugin.ReattachConfig{
		Protocol:        plugin.Protocol(r.Protocol),
		ProtocolVersion: int(r.ProtocolVersion),
		Addr: &SimpleAddr{
			NetworkString: r.Addr.Network,
			AddressString: r.Addr.Address,
		},
		Pid: int(r.Pid),
	}
}
