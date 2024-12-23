package types

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type Table struct {
	Name   string
	Schema *schema.RowSchema
}

func TableFromProto(pt *proto.Table) *Table {
	return &Table{
		Name:   pt.Name,
		Schema: schema.RowSchemaFromProto(pt.Schema),
	}
}

type CollectRequest struct {
	TableName     string
	PartitionName string

	// unique identifier for collection execution this will be used as base for the filename fo the resultiung JSONL files
	ExecutionId string
	// dest path for jsonl files
	OutputPath string
	// the source to use (with raw config)
	SourceData *SourceConfigData
	// the source format to use (with raw config)
	SourceFormat *FormatConfigData
	// the raw hcl of the connection
	ConnectionData *ConnectionConfigData
	// this is json encoded data that represents the state of the collection, i.e. what data has been collected
	// this is used to resume a collection
	CollectionState []byte

	CustomTable *Table
}

func CollectRequestFromProto(pr *proto.CollectRequest) (*CollectRequest, error) {
	if pr.SourceData == nil {
		return nil, fmt.Errorf("source data is required")
	}
	sourceData, err := DataFromProto[*SourceConfigData](pr.SourceData)
	if err != nil {
		return nil, err
	}

	// NOTE: add the (possibly nil) SourcePluginReattach to the source data
	sourceData.SetReattach(pr.SourcePlugin)

	req := &CollectRequest{
		TableName:       pr.TableName,
		PartitionName:   pr.PartitionName,
		ExecutionId:     pr.ExecutionId,
		OutputPath:      pr.OutputPath,
		SourceData:      sourceData,
		CollectionState: pr.CollectionState,
	}

	if pr.SourceFormat != nil {
		sourceFormat, err := DataFromProto[*FormatConfigData](pr.SourceFormat)
		if err != nil {
			return nil, err
		}
		req.SourceFormat = sourceFormat
	}

	if pr.ConnectionData != nil {
		connectionData, err := DataFromProto[*ConnectionConfigData](pr.ConnectionData)
		if err != nil {
			return nil, err
		}
		req.ConnectionData = connectionData
	}
	if pr.CustomTable != nil {
		req.CustomTable = TableFromProto(pr.CustomTable)
	}

	return req, nil
}
