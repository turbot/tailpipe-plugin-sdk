package types

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"time"
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

// CollectRequest is an sdk type which is mapped from the proto.CollectRequest
type CollectRequest struct {
	TableName     string
	PartitionName string

	// unique identifier for collection execution this will be used as base for the filename fo the resultiung JSONL files
	ExecutionId string
	// the parent folder for all collection related files (JSONL files, temp source files)
	CollectionTempDir string
	// the folder containing collection state files (e.g. last collection time)
	CollectionStateDir string
	// the source to use (with raw config)
	SourceData *SourceConfigData
	// the source format to use (with raw config)
	SourceFormat *FormatConfigData
	// the raw hcl of the connection
	ConnectionData *ConnectionConfigData
	// the collection start time
	From time.Time
	// the custom table definition, if specified
	CustomTable *Table
}

func CollectRequestFromProto(pr *proto.CollectRequest) (*CollectRequest, error) {
	if pr.SourceData == nil {
		return nil, fmt.Errorf("source data is required")
	}
	sourceData, err := ConfigDataFromProto[*SourceConfigData](pr.SourceData)
	if err != nil {
		return nil, err
	}

	// NOTE: add the (possibly nil) SourcePluginReattach to the source data
	sourceData.SetReattach(pr.SourcePlugin)

	req := &CollectRequest{
		TableName:          pr.TableName,
		PartitionName:      pr.PartitionName,
		ExecutionId:        pr.ExecutionId,
		CollectionTempDir:  pr.CollectionTempDir,
		CollectionStateDir: pr.CollectionStateDir,
		SourceData:         sourceData,
		From:               pr.FromTime.AsTime(),
	}

	if pr.SourceFormat != nil {
		sourceFormat, err := ConfigDataFromProto[*FormatConfigData](pr.SourceFormat)
		if err != nil {
			return nil, err
		}
		req.SourceFormat = sourceFormat
	}

	if pr.ConnectionData != nil {
		connectionData, err := ConfigDataFromProto[*ConnectionConfigData](pr.ConnectionData)
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
