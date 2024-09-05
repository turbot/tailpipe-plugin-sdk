package collection_state

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// AwsS3CollectionState contains the collection state data for an AWS S3 Bucket artifact source
type AwsS3CollectionState struct {
	ArtifactCollectionState[*artifact_source_config.AwsS3BucketSourceConfig]

	StartAfterKey    *string `json:"start_after_key,omitempty"`
	UseStartAfterKey bool    `json:"use_start_after_key"`
}

func NewAwsS3CollectionState() CollectionState[*artifact_source_config.AwsS3BucketSourceConfig] {
	return &AwsS3CollectionState{}
}

func (s *AwsS3CollectionState) Init(config *artifact_source_config.AwsS3BucketSourceConfig) error {
	s.UseStartAfterKey = config.LexicographicalOrder
	return nil
}

// IsEmpty returns true if the collection state is empty
func (s *AwsS3CollectionState) IsEmpty() bool {
	return s.StartAfterKey == nil && s.StartTime.IsZero()
}

// Upsert adds new/updates an existing object with its current metadata
// Note: the object name is the full path to the object
func (s *AwsS3CollectionState) Upsert(metadata *types.ArtifactInfo) {
	s.ArtifactCollectionState.Upsert(metadata)

	if s.UseStartAfterKey {
		s.StartAfterKey = &metadata.Name
	}
}
