package artifact_loader

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

const NullLoaderIdentifier = "null_loader"

// NullLoader is an Loader that does nothing - it does not extract rows so will generate no row events
// this is used for tables which process the artifact directly, e.g. cav table which converts the csv directly to JSONL
type NullLoader struct {
}

func NewNullLoader() Loader {
	return &NullLoader{}
}

func (g NullLoader) Identifier() string {
	return NullLoaderIdentifier
}

// Load implements [Loader]
// Extracts an object from a  file
func (g NullLoader) Load(_ context.Context, _ *types.DownloadedArtifactInfo, dataChan chan *types.RowData) error {
	// just close the channel
	close(dataChan)

	return nil
}
