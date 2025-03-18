package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DescribeResponse struct {
	Schemas       schema.SchemaMap
	Sources       row_source.SourceMetadataMap
	FormatPresets formats.FormatDescriptionMap
	CustomFormats formats.FormatDescriptionMap
	FormatTypes   []string
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
		res.Sources = row_source.SourceMetadataMapFromProto(resp.Sources)
	}
	if resp.FormatsPresets != nil {
		res.FormatPresets = formats.FormatMapFromProto(resp.FormatsPresets)
	}
	if resp.CustomFormats != nil {
		res.CustomFormats = formats.FormatMapFromProto(resp.CustomFormats)
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
