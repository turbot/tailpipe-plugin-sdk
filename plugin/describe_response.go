package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DescribeResponse struct {
	Schemas schema.SchemaMap
	Sources row_source.SourceMetadataMap
	Formats formats.FormatDescriptionMap
}

func (d *DescribeResponse) ToProto() *proto.DescribeResponse {
	return &proto.DescribeResponse{
		Schemas: d.Schemas.ToProto(),
		Sources: d.Sources.ToProto(),
		Formats: d.Formats.ToProto(),
	}
}
