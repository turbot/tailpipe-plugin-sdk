package row_source

import (
	"encoding/json"
)

// RowSourceOption is a function that can be used to configure a RowSource
// NOTE: individual options are specific to specific row source types
// RowSourceOption accepts the base Observable interface,
// and each option must implement a safe type assertion to the specific row source type
type RowSourceOption func(source RowSource) error

func WithCollectionStateJSON(collectionStateJSON json.RawMessage) RowSourceOption {
	return func(source RowSource) error {
		return source.SetCollectionStateJSON(collectionStateJSON)
	}
}
