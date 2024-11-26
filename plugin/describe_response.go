package plugin

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type DescribeResponse struct {
	Schemas schema.SchemaMap
	Sources row_source.SourceMetadataMap
}

func (d *DescribeResponse) ToProto() *proto.DescribeResponse {
	return &proto.DescribeResponse{
		Schemas: d.Schemas.ToProto(),
		Sources: d.Sources.ToProto(),
	}
}
