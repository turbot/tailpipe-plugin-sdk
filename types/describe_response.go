package types

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DescribeResponse struct {
	// proto fields
	Schemas       schema.SchemaMap
	Sources       SourceMetadataMap
	FormatPresets FormatDescriptionMap
	CustomFormats FormatDescriptionMap
	FormatTypes   []string

	// non-proto fields - should be populated by PluginManager before returning
	PluginName string
}

func (d *DescribeResponse) ToProto() *proto.DescribeResponse {
	return &proto.DescribeResponse{
		Schemas:        d.Schemas.ToProto(),
		Sources:        d.Sources.ToProto(),
		FormatsPresets: d.FormatPresets.ToProto(),
		CustomFormats:  d.CustomFormats.ToProto(),
		FormatTypes:    d.FormatTypes,
	}
}

func DescribeResponseFromProto(resp *proto.DescribeResponse) *DescribeResponse {
	res := &DescribeResponse{}
	if resp == nil {
		return res
	}
	if resp.Schemas != nil {
		res.Schemas = schema.SchemaMapFromProto(resp.Schemas)
	}
	if resp.Sources != nil {
		res.Sources = SourceMetadataMapFromProto(resp.Sources)
	}
	if resp.FormatsPresets != nil {
		res.FormatPresets = FormatMapFromProto(resp.FormatsPresets)
	}
	if resp.CustomFormats != nil {
		res.CustomFormats = FormatMapFromProto(resp.CustomFormats)
	}
	res.FormatTypes = resp.FormatTypes

	return res
}

func (d *DescribeResponse) AsMetadataMap() map[string][]string {
	metadata := make(map[string][]string)

	for _, tableValue := range d.Schemas {
		metadata["tables"] = append(metadata["tables"], tableValue.Name)
	}

	for _, sourceValue := range d.Sources {
		metadata["sources"] = append(metadata["sources"], sourceValue.Name)
	}

	for _, preset := range d.FormatPresets {
		metadata["format_presets"] = append(metadata["format_presets"], preset.FullName())
	}

	metadata["format_types"] = d.FormatTypes

	return metadata
}
