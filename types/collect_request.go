package types

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"time"
)

// CollectRequest is an sdk type which is mapped from the proto.CollectRequest
type CollectRequest struct {
	TableName     string
	PartitionName string

	// unique identifier for collection execution this will be used as base for the filename fo the resultiung JSONL files
	ExecutionId string
	// the parent folder for all collection related files (JSONL files, temp source files)
	CollectionTempDir string
	// the filepath for the collection state json file
	CollectionStatePath string
	// the source to use (with raw config)
	SourceData *SourceConfigData
	// the source format to use (with either raw hcl config, or the preset name)
	SourceFormat *FormatConfigData
	// the raw hcl of the connection
	ConnectionData *ConnectionConfigData
	// the collection start time
	From time.Time
	// the collection end time
	To time.Time
	// the custom table definition, if specified
	CustomTableSchema *schema.TableSchema
	// the max space to take with temp files
	TempDirMaxMb int64
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
	if pr.SourcePlugin != nil {
		sourceData.SetReattach(pr.SourcePlugin)
	}

	req := &CollectRequest{
		TableName:           pr.TableName,
		PartitionName:       pr.PartitionName,
		ExecutionId:         pr.ExecutionId,
		CollectionTempDir:   pr.CollectionTempDir,
		CollectionStatePath: pr.CollectionStatePath,
		SourceData:          sourceData,
		TempDirMaxMb:        pr.TempDirMaxMb,
	}

	if pr.FromTime != nil {
		req.From = pr.FromTime.AsTime()
	}
	if pr.ToTime != nil {
		req.To = pr.ToTime.AsTime()
	}

	if pr.SourceFormat != nil {
		sourceFormat, err := FormatConfigDataFromProto(pr.SourceFormat)
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
	if pr.CustomTableSchema != nil {
		req.CustomTableSchema = schema.TableSchemaFromProto(pr.CustomTableSchema)
	}

	return req, nil
}
