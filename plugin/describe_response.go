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
	Formats       formats.FormatDescriptionMap
	CustomFormats formats.FormatDescriptionMap
}

func (d *DescribeResponse) ToProto() *proto.DescribeResponse {
	return &proto.DescribeResponse{
		Plugin:        d.Plugin,
		Schemas:       d.Schemas.ToProto(),
		Sources:       d.Sources.ToProto(),
		Formats:       d.Formats.ToProto(),
		CustomFormats: d.CustomFormats.ToProto(),
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
	if resp.Formats != nil {
		res.Formats = formats.FormatMapFromProto(resp.Formats)
	}
	return res
}
