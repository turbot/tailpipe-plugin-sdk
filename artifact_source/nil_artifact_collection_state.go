package artifact_source

import (
	"time"
)

// NilArtifactCollectionState is a collection state that does nothing
// it is used by PluginSourceWrapper - as the actual collection state is implemented by the source plugin
type NilArtifactCollectionState struct {
}

func (*NilArtifactCollectionState) Init(_ *NilArtifactSourceConfig, _ string) error {
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

func (*NilArtifactCollectionState) Save() error {
	return nil
}
