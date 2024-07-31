package artifact_row_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Source is an interface providing methods for discovering and downloading artifacts to the local file system
// an [row_source.Base] must be configured to have a Source implementation.
// A Source can optionally specify a [Mapper] that should be used to extract data from the artifact
// Sources provided by the SDK: [FileSystemSource], [AwsS3BucketSource], [AwsCloudWatchSource]
type Source interface {
	row_source.RowSource
	DiscoverArtifacts(ctx context.Context) error
	DownloadArtifact(context.Context, *types.ArtifactInfo) error
	// Mapper returns the name of a mapper that should be used to extract data from the artifact
	// this should be provided in the case of sources which require specific mapping./extraction, e.g. Cloudwatch
	// artifact.Base provides an empty implementation
	//Mapper() func() artifact_mapper.Mapper
}
