package artifact_source

import (
	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"time"
)

// NilArtifactCollectionState is a collection state that does nothing
// it is used by PluginSourceWrapper - as the actual collection state is implemented by the source plugin
type NilArtifactCollectionState struct {
}

func (s *NilArtifactCollectionState) GetFromTime() time.Time {
	return time.Time{}
}

func (s *NilArtifactCollectionState) GetToTime() time.Time {
	return time.Time{}
}

func (s *NilArtifactCollectionState) SetEndTime(_ time.Time) {
}

func (*NilArtifactCollectionState) Init(*collection_state.CollectionTimeRange) error {
	return nil
}

func (s *NilArtifactCollectionState) RegisterPath(_ string, _ map[string]string) {
}

func (*NilArtifactCollectionState) ShouldCollect(_ string, _ time.Time) bool {
	return false
}

func (*NilArtifactCollectionState) OnCollected(_ string, _ time.Time) error {
	return nil
}

func (*NilArtifactCollectionState) SetGranularity(_ time.Duration) {
}

func (*NilArtifactCollectionState) GetGranularity() time.Duration {
	return 0
}

func (*NilArtifactCollectionState) IsEmpty() bool {
	return true
}

func (*NilArtifactCollectionState) OnCollectionComplete() error {
	return nil
}

func (*NilArtifactCollectionState) Save() error {
	return nil
}

func (*NilArtifactCollectionState) MigrateFromLegacyState(_ []byte) error {
	return nil
}
func (*NilArtifactCollectionState) Validate() error {
	return nil
}
func (*NilArtifactCollectionState) Clear(_ *collection_state.CollectionTimeRange) {
}
