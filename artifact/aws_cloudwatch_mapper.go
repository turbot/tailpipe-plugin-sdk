package artifact

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
)

// CloudwatchMapper is an Mapper that receives AWSCloudTrailBatch objects and extracts AWSCloudTrail records from them
type CloudwatchMapper struct {
}

// NewCloudwatchMapper creates a new CloudwatchMapper
func NewCloudwatchMapper() Mapper {
	return &CloudwatchMapper{}
}

func (c *CloudwatchMapper) Identifier() string {
	return AWSCloudwatchMapperIdentifier
}

// Map casts the data item as a map and extracts the data and cloudtrail metadata
func (c *CloudwatchMapper) Map(_ context.Context, a *ArtifactData) ([]*ArtifactData, error) {
	// the expected input type is a JSON string deserializable to a map with keys "IngestionTime", "Timestamp" and "Message"
	jsonString, ok := a.Data.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", a)
	}

	var cloudtrailEntry map[string]any
	err := json.Unmarshal([]byte(jsonString), &cloudtrailEntry)
	if err != nil {
		return nil, fmt.Errorf("error decoding json: %w", err)
	}
	row := cloudtrailEntry["Message"].(string)
	metadata := a.Metadata.Clone()
	metadata.TpIngestTimestamp = helpers.UnixMillis(cloudtrailEntry["IngestionTime"].(float64))
	metadata.TpTimestamp = helpers.UnixMillis(cloudtrailEntry["Timestamp"].(float64))

	d := NewData(row, WithMetadata(metadata))

	return []*ArtifactData{d}, nil
}
