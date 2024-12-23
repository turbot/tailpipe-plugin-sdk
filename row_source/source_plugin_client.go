package row_source

import (
	"context"
	"encoding/json"

	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	pluginshared "github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"io/ioutil"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
)

// SourcePluginClient is the client object used by clients of the plugin
type SourcePluginClient struct {
	Name   string
	Stub   pluginshared.TailpipePluginClientWrapper
	client *plugin.Client
}

func NewPluginClient(client *plugin.Client, pluginName string) (*SourcePluginClient, error) {
	log.Printf("[TRACE] NewPluginClient for plugin %s", pluginName)

	// connect via RPC
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
	p := raw.(pluginshared.TailpipePluginClientWrapper)
	res := &SourcePluginClient{
		Name:   pluginName,
		Stub:   p,
		client: client,
	}
	return res, nil
}

func NewPluginClientFromReattach(sourcePlugin *types.SourcePluginReattach) (*SourcePluginClient, error) {
	log.Printf("[TRACE] NewPluginClientFromReattach for plugin %s", sourcePlugin.Plugin)
	// create the plugin map
	pluginMap := map[string]plugin.Plugin{
		sourcePlugin.Plugin: &pluginshared.TailpipeGRPCPlugin{},
	}
	// discard logging from the client (plugin logs will still flow through to the log file as the plugin manager set this up)
	logger := logging.NewLogger(&hclog.LoggerOptions{Name: "plugin", Output: ioutil.Discard})

	// create grpc client
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  pluginshared.Handshake,
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
func (c *SourcePluginClient) Exited() bool {
	return c.client.Exited()
}

func (c *SourcePluginClient) AddObserver() (proto.TailpipePlugin_AddObserverClient, error) {
	return c.Stub.AddObserver()
}

func (c *SourcePluginClient) Init(_ context.Context, sourceConfig types.ConfigData, connectionConfig types.ConfigData, opts ...RowSourceOption) error {
	req := &proto.InitRequest{
		SourceConfig:     sourceConfig.AsProto(),
		ConnectionConfig: connectionConfig.AsProto(),
	}

	// TODO how do we handle opts????
	_, err := c.Stub.Init(req)
	return err
}

func (c *SourcePluginClient) Close() error {
	_, err := c.Stub.Close()
	return err
}

func (c *SourcePluginClient) Collect(_ context.Context) error {
	_, err := c.Stub.SourceCollect(&proto.SourceCollectRequest{})
	return err

}

func (c *SourcePluginClient) GetCollectionStateJSON() (json.RawMessage, error) {
	panic("implement me")
}

func (c *SourcePluginClient) SetCollectionStateJSON(stateJSON json.RawMessage) error {
	panic("implement me")
}

func (c *SourcePluginClient) GetTiming() types.TimingCollection {
	panic("implement me")
	//resp, err := c.Stub.GetSourceTiming()
	//if err != nil {
	//	log.Printf("[ERROR] GetTiming failed: %s", err.Error())
	//	return types.TimingCollection{}
	//}
	//return types.TimingCollection{
}

func (c *SourcePluginClient) Describe() (*proto.DescribeResponse, error) {
	return c.Stub.Describe()
}
