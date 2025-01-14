package artifact_source

import (
	"context"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_loader"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// ArtifactSource is an interface providing methods for discovering and downloading artifacts to the local file system
// an [row_source.RowSourceImpl] must be configured to have a ArtifactSource implementation.
// Sources provided by the SDK: [FileSystemSource], [AwsS3BucketSource], [AwsCloudWatchSource]
type ArtifactSource interface {
	row_source.RowSource

	DiscoverArtifacts(ctx context.Context) error
	DownloadArtifact(context.Context, *types.ArtifactInfo) error

	// functions to support RowSourceOptions

	SetExtractor(extractor Extractor)
	SetLoader(loader artifact_loader.Loader)
	SetRowPerLine(b bool)
	SetSkipHeaderRow(b bool)
	SetDefaultConfig(config *artifact_source_config.ArtifactSourceConfigImpl)
}

// Extractor is an interface which provides a method for extracting rows from an artifact
type Extractor interface {
	Identifier() string
	// Extract retrieves one more more rows from the artifact data
	Extract(context.Context, any) ([]any, error)
}
