package artifact_source

import (
	"context"

	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// Source is an interface providing methods for discovering and downloading artifacts to the local file system
// an [row_source.ArtifactRowSource] must be configured to have a Source implementation.
// A Source can optionally specify a [Mapper] that should be used to extract data from the artifact
// Sources provided by the SDK: [FileSystemSource], [AwsS3BucketSource], [AwsCloudWatchSource]
type Source interface {
	observable.Observable

	Identifier() string

	// Init is called when the source is created
	// it is responsible for parsing the source config and configuring the source
	Init(ctx context.Context, config *hcl.Data) error

	Close() error

	DiscoverArtifacts(ctx context.Context) error

	DownloadArtifact(context.Context, *types.ArtifactInfo) error
}
