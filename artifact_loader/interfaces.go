package artifact_loader

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Loader is an interface which provides a method for loading a locally saved artifact
// Sources provided by the SDK: [GzipLoader], [GzipRowLoader], [FileSystemLoader], [FileSystemRowLoader]
type Loader interface {
	Identifier() string
	// Load locally saved artifact data and perform any necessary decompression/decryption
	Load(context.Context, *types.ArtifactInfo, chan *types.RowData) error
}
