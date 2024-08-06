package artifact_mapper

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const (
	AWSCloudwatchMapperIdentifier = "aws_cloudwatch_mapper"
)

// CloudwatchMapper is an [plugin.Mapper] implementation
// that receives JSON data and returns AWSCloudTrail records
type CloudwatchMapper struct {
}

// NewCloudwatchMapper creates a new CloudwatchMapper
func NewCloudwatchMapper() Mapper {
	return &CloudwatchMapper{}
}

func (c *CloudwatchMapper) Identifier() string {
	return AWSCloudwatchMapperIdentifier
}

// Map unmarshalls JSON into an AWSCloudTrailBatch object and extracts AWSCloudTrail records from it
func (c *CloudwatchMapper) Map(_ context.Context, a *types.RowData) ([]*types.RowData, error) {
	// TODO: #mapper when using the row per line on artifact source (i.e. lambda logs in CloudWatch), the data is a string, maybe we need a better approach to handle this
	if _, isString := a.Data.(string); isString {
		a.Data = []byte(a.Data.(string))
	}
	// TODO #mapper make this more resilient to input type
	// the expected input type is a JSON string deserializable to a map with keys "IngestionTime", "Timestamp" and "Message"
	jsonBytes, ok := a.Data.([]byte)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", a.Data)
	}

	var cloudwatchEntry map[string]any
	err := json.Unmarshal(jsonBytes, &cloudwatchEntry)
	if err != nil {
		return nil, fmt.Errorf("error decoding json: %w", err)
	}
	msg, ok := cloudwatchEntry["Message"]
	if !ok {
		return nil, fmt.Errorf("expected key 'Message' in cloudwatch log entry")
	}
	row := msg.(string)
	metadata := a.Metadata.Clone()
	metadata.TpIngestTimestamp = helpers.UnixMillis(cloudwatchEntry["IngestionTime"].(float64))
	metadata.TpTimestamp = helpers.UnixMillis(cloudwatchEntry["Timestamp"].(float64))

	d := types.NewData(row, types.WithMetadata(metadata))

	return []*types.RowData{d}, nil
}
