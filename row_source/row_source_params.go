package row_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type RowSourceParams struct {
	SourceConfigData    *types.SourceConfigData
	ConnectionData      *types.ConnectionConfigData
	CollectionStatePath string
	From                time.Time
	To                  time.Time
	CollectionTempDir   string
	Overwrite           bool
}

func (r *RowSourceParams) ToProto() *proto.RowSourceParams {
	res := &proto.RowSourceParams{
		CollectionStatePath: r.CollectionStatePath,
		FromTime:            timestamppb.New(r.From),
		ToTime:              timestamppb.New(r.To),
		CollectionTempDir:   r.CollectionTempDir,
		Overwrite:           r.Overwrite,
	}
	if r.SourceConfigData != nil {
		res.SourceData = r.SourceConfigData.ToProto()
	}
	if r.ConnectionData != nil {
		res.ConnectionData = r.ConnectionData.ToProto()
	}
	return res
}

func RowSourceParamsFromProto(pr *proto.RowSourceParams) (*RowSourceParams, error) {
	res := &RowSourceParams{
		CollectionStatePath: pr.CollectionStatePath,
		From:                pr.FromTime.AsTime(),
		To:                  pr.ToTime.AsTime(),
		CollectionTempDir:   pr.CollectionTempDir,
		Overwrite:           pr.Overwrite,
	}

	if pr.SourceData != nil {
		sourceData, err := types.ConfigDataFromProto[*types.SourceConfigData](pr.SourceData)
		if err != nil {
			return nil, err
		}
		res.SourceConfigData = sourceData
	}

	if pr.ConnectionData != nil {
		connectionData, err := types.ConfigDataFromProto[*types.ConnectionConfigData](pr.ConnectionData)
		if err != nil {
			return nil, err
		}
		res.ConnectionData = connectionData
	}
	return res, nil
}
