package table

import (
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type CollectData struct {
	TableConfigData  *types.ConfigData
	SourceConfigData *types.ConfigData
	ConnectionData   *types.ConfigData
	CollectionState  []byte
}

func newCollectionConfigData(req *proto.CollectRequest) *CollectData {
	return &CollectData{

		// convert req into tableConfigData and sourceConfigData
		TableConfigData:  types.DataFromProto(req.TableData),
		SourceConfigData: types.DataFromProto(req.SourceData),
		ConnectionData:   types.DataFromProto(req.ConnectionData),
		CollectionState:  req.CollectionState,
	}
}
