package grpc

import (
	"io"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// PluginClient is the client object used by clients of the plugin
type PluginClient struct {
	shared.TailpipePluginClientWrapper
	Name   string
	Client *plugin.Client
}

func NewPluginClient(client *plugin.Client, pluginName string) (*PluginClient, error) {
	// connect via GRPC
	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}

	// request the plugin
	raw, err := rpcClient.Dispense(pluginName)
	if err != nil {
		return nil, err
	}
	// we should have a stub plugin now
	res := &PluginClient{
		TailpipePluginClientWrapper: *(raw.(*shared.TailpipePluginClientWrapper)),
		Name:                        pluginName,
		Client:                      client,
	}
	return res, nil
}

func NewPluginClientFromReattach(sourcePlugin *types.SourcePluginReattach) (*PluginClient, error) {
	// create the plugin map
	pluginMap := map[string]plugin.Plugin{
		sourcePlugin.Plugin: &shared.TailpipeGRPCPlugin{},
	}
	// discard logging from the client (plugin logs will still flow through to the log file as the plugin manager set this up)
	logger := logging.NewLogger(&hclog.LoggerOptions{Name: "plugin", Output: io.Discard})

	// create grpc client
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  shared.Handshake,
		Plugins:          pluginMap,
		Reattach:         sourcePlugin.Reattach,
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		Logger:           logger,
	})
	res, err := NewPluginClient(client, sourcePlugin.Plugin)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Exited returned whether the underlying client has exited, i.e. the plugin has terminated
func (c *PluginClient) Exited() bool {
	return c.Client.Exited()
}
