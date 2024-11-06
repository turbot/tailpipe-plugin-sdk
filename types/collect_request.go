package types

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/config_data"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type CollectRequest struct {
	// unique identifier for collection execution this will be used as base for the filename fo the resultiung JSONL files
	ExecutionId string
	// dest path for jsonl files
	OutputPath string `protobuf:"bytes,2,opt,name=output_path,json=outputPath,proto3" json:"output_path,omitempty"`
	// the table to collect (with raw config)
	PartitionData *config_data.PartitionConfigData
	// the source to use (with raw config)
	SourceData *config_data.SourceConfigData
	// the raw hcl of the connection
	ConnectionData *config_data.ConnectionConfigData
	// this is json encoded data that represents the state of the collection, i.e. what data has been collected
	// this is used to resume a collection
	CollectionState []byte
}

func CollectRequestFromProto(pr *proto.CollectRequest) (*CollectRequest, error) {
	if pr.PartitionData == nil {
		return nil, fmt.Errorf("partition data is required")
	}
	partitionData, err := config_data.DataFromProto[*config_data.PartitionConfigData](pr.PartitionData)
	if err != nil {
		return nil, err
	}
	if pr.SourceData == nil {
		return nil, fmt.Errorf("source data is required")
	}
	sourceData, err := config_data.DataFromProto[*config_data.SourceConfigData](pr.SourceData)
	if err != nil {
		return nil, err
	}

	req := &CollectRequest{
		ExecutionId:     pr.ExecutionId,
		OutputPath:      pr.OutputPath,
		PartitionData:   partitionData,
		SourceData:      sourceData,
		CollectionState: pr.CollectionState,
	}
	if pr.ConnectionData != nil {
		connectionData, err := config_data.DataFromProto[*config_data.ConnectionConfigData](pr.ConnectionData)
		if err != nil {
			return nil, err
		}
		req.ConnectionData = connectionData
	}
	return req, nil
}
