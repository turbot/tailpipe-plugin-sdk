package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"time"
)

// NilCollectionState is a collection state that does nothing
// it is used by PluginSourceWrapper - as the actual collection state is implemented by the source plugin
type NilCollectionState struct {
	collection_state.CollectionStateBase
}

func (*NilCollectionState) Init(*NilArtifactSourceConfig) error {
	return nil
}

func (*NilCollectionState) ShouldCollect(*types.ArtifactInfo) bool {
	return false
}

func (*NilCollectionState) OnCollected(*types.ArtifactInfo) error {
	return nil
}

func (*NilCollectionState) GetGranularity() time.Duration {
	return 0
}
