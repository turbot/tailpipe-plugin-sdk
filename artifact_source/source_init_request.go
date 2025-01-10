package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// InitSourceRequest is an sdk type which is mapped from the proto.InitSourceRequest
type InitSourceRequest struct {
	// the source format to use (with raw config)
	SourceFormat  *types.FormatConfigData
	SourceParams  *row_source.RowSourceParams
	DefaultConfig *artifact_source_config.ArtifactSourceConfigBase
}

func InitSourceRequestFromProto(pr *proto.InitSourceRequest) (*InitSourceRequest, error) {
	params, err := row_source.RowSourceParamsFromProto(pr.SourceParams)
	if err != nil {
		return nil, err
	}
	req := &InitSourceRequest{
		SourceParams: params,
	}

	if pr.DefaultConfig != nil {
		defaultConfig := artifact_source_config.ArtifactSourceConfigBaseFromProto(pr.DefaultConfig)
		req.DefaultConfig = defaultConfig
	}

	return req, nil
}
