package types

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type CollectRequest struct {

	// unique identifier for collection execution this will be used as base for the filename fo the resultiung JSONL files
	ExecutionId string
	// dest path for jsonl files
	OutputPath string `protobuf:"bytes,2,opt,name=output_path,json=outputPath,proto3" json:"output_path,omitempty"`
	// the partition to collect (with raw config)
	TableData *ConfigData
	// the source to use (with raw config)
	SourceData *ConfigData
	// the raw hcl of the connection
	ConnectionData *ConfigData
	// this is json encoded data that represents the state of the collection, i.e. what data has been collected
	// this is used to resume a collection
	CollectionState []byte
}

func CollectRequestFromProto(data *proto.CollectRequest) *CollectRequest {
	return &CollectRequest{
		ExecutionId:     data.ExecutionId,
		OutputPath:      data.OutputPath,
		TableData:       DataFromProto(data.TableData),
		SourceData:      DataFromProto(data.SourceData),
		ConnectionData:  DataFromProto(data.ConnectionData),
		CollectionState: data.CollectionState,
	}
}
