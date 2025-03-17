package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"
)

// PluginFormatWrapper is an implementation of Format which wraps a GRPC plugin which implements the format
type PluginFormatWrapper struct {
	client           *grpc.PluginClient
	pluginName       string
	describeResponse *FormatDescription
}

// NewPluginFormatWrapper creates a new PluginFormatWrapper
func NewPluginFormatWrapper(formatData *types.FormatConfigData, sourcePlugin *types.SourcePluginReattach) (*PluginFormatWrapper, error) {
	res := &PluginFormatWrapper{}
	err := res.SetPlugin(sourcePlugin)
	if err != nil {
		return nil, err
	}

	// convert formatData back to proto
	fp := &proto.FormatData{
		Name:   formatData.Name,
		Config: formatData.ToProto(),
	}

	// describe the format
	describeReq := &proto.DescribeRequest{
		CustomFormats:     []*proto.FormatData{fp},
		CustomFormatsOnly: true,
	}
	describeResp, err := res.client.Describe(describeReq)
	if err != nil {
		return nil, err
	}

	slog.Debug("PluginFormatWrapper - describe response", "response", describeResp.CustomFormats, "formatData.InstanceType", formatData.InstanceType, "describeResp.CustomFormats[formatData.InstanceType]", describeResp.CustomFormats[formatData.InstanceType])
	// we expect the first format to be the one we asked for
	desc, ok := describeResp.CustomFormats[formatData.InstanceType]
	if !ok {
		return nil, fmt.Errorf("plugin returned no description returned for format %s", formatData.InstanceType)
	}
	res.describeResponse = FormatDescriptionFromProto(desc)

	return res, nil
}

func (w *PluginFormatWrapper) Validate() error {
	// if we managed to describe the format, we can assume it is valid
	return nil
}

func (w *PluginFormatWrapper) Identifier() string {
	return w.describeResponse.Type
}

func (w *PluginFormatWrapper) GetName() string {
	return w.describeResponse.Name
}

func (w *PluginFormatWrapper) SetName(string) {
	// not required for wrapper
}

func (w *PluginFormatWrapper) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	return mappers.NewRegexMapper[*types.DynamicRow](w.describeResponse.Regex)
}

func (w *PluginFormatWrapper) GetRegex() (string, error) {
	return w.describeResponse.Regex, nil
}

func (w *PluginFormatWrapper) GetDescription() string {
	return w.describeResponse.Description
}

func (w *PluginFormatWrapper) GetProperties() map[string]string {
	return w.describeResponse.Properties
}

// SetPlugin sets the plugin client for the source
// this is called from WithPluginReattach option
func (w *PluginFormatWrapper) SetPlugin(sourcePlugin *types.SourcePluginReattach) error {
	client, err := grpc.NewPluginClientFromReattach(sourcePlugin)
	if err != nil {
		return err
	}
	w.client = client
	w.pluginName = sourcePlugin.Plugin
	return nil
}
