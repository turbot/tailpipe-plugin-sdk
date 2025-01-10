package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"time"
)

// NilArtifactCollectionState is a collection state that does nothing
// it is used by PluginSourceWrapper - as the actual collection state is implemented by the source plugin
type NilArtifactCollectionState struct {
	collection_state.CollectionStateImpl[NilArtifactSourceConfig]
}

func (*NilArtifactCollectionState) Init(_ *NilArtifactSourceConfig, _ string) error {
	return nil
}

func (*NilArtifactCollectionState) ShouldCollect(_ collection_state.SourceItemMetadata) bool {
	return false
}

func (*NilArtifactCollectionState) OnCollected(_ collection_state.SourceItemMetadata) error {
	return nil
}

func (*NilArtifactCollectionState) GetGranularity() time.Duration {
	return 0
}
