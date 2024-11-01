package artifact_mapper

import (
	"context"
	"encoding/json"
	"fmt"
)

const (
	AWSCloudwatchMapperIdentifier = "aws_cloudwatch_mapper"
)

// CloudwatchMapper is an [plugin.Mappers] implementation
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
func (c *CloudwatchMapper) Map(_ context.Context, a any) ([]any, error) {
	// when using the row per line on artifact source (i.e. lambda logs in CloudWatch), the data is a string
	if _, isString := a.(string); isString {
		a = []byte(a.(string))
	}
	// TODO #mapper make this more resilient to input type https://github.com/turbot/tailpipe-plugin-sdk/issues/2
	// the expected input type is a JSON string deserializable to a map with keys "IngestionTime", "Timestamp" and "Message"
	jsonBytes, ok := a.([]byte)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", a)
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

	// TODO fix this
	//metadata.TpIngestTimestamp = helpers.UnixMillis(cloudwatchEntry["IngestionTime"].(float64))
	//metadata.TpTimestamp = helpers.UnixMillis(cloudwatchEntry["Timestamp"].(float64))

	return []any{row}, nil
}
