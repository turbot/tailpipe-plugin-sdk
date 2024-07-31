package artifact_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_mapper"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/paging"
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
	// SetPagingData is called by the parent ArtifactRowSource - set the paging data for the source
	SetPagingData(data paging.Data)
	Close() error
	DiscoverArtifacts(ctx context.Context) error
	DownloadArtifact(context.Context, *types.ArtifactInfo) error
	GetPagingDataSchema() paging.Data
	// Mapper returns the name of a mapper that should be used to extract data from the artifact
	// this should be provided in the case of sources which require specific mapping./extraction, e.g. Cloudwatch
	// artifact.Base provides an empty implementation
	Mapper() func() artifact_mapper.Mapper
}
