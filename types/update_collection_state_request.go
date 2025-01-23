package types

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"time"
)

// UpdateCollectionStateRequest is an sdk type which is mapped from the proto.UpdateCollectionStateRequest
type UpdateCollectionStateRequest struct {
	// the folder containing collection state files (e.g. last collection time)
	CollectionStatePath string
	// the source to use (with raw config)
	SourceData *SourceConfigData
	// the start time - this will be the new state end time
	From time.Time
}

func UpdateCollectionStateRequestFromProto(pr *proto.UpdateCollectionStateRequest) (*UpdateCollectionStateRequest, error) {
	if pr.SourceData == nil {
		return nil, fmt.Errorf("source data is required")
	}
	sourceData, err := ConfigDataFromProto[*SourceConfigData](pr.SourceData)
	if err != nil {
		return nil, err
	}

	res := &UpdateCollectionStateRequest{
		CollectionStatePath: pr.CollectionStatePath,
		SourceData:          sourceData,
		From:                pr.FromTime.AsTime(),
	}
	return res, nil
}
