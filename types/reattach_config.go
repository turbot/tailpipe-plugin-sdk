package types

import (
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type SourcePluginReattach struct {
	Reattach *plugin.ReattachConfig

	Plugin     string
	SourceType string
}

func ReattachFromProto(r *proto.SourcePluginReattach) *SourcePluginReattach {
	return &SourcePluginReattach{
		Reattach:   r.ReattachConfig.ToPluginReattach(),
		Plugin:     r.Plugin,
		SourceType: r.SourceType,
	}
}
