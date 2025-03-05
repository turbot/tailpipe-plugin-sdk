package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DescribeResponse struct {
	Plugin        string
	Schemas       schema.SchemaMap
	Sources       row_source.SourceMetadataMap
	FormatPresets formats.FormatDescriptionMap
	CustomFormats formats.FormatDescriptionMap
	FormatTypes   []string
}

func (d *DescribeResponse) ToProto() *proto.DescribeResponse {
	return &proto.DescribeResponse{
		Plugin:         d.Plugin,
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
