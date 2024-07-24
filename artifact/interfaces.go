package artifact

import (
	"context"
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
	Close() error

	// Mapper returns the mapper that should be used to extract data from the artifact
	// this should be provided in the case of sources which require specific mapping./extraction, e.g. Cloudwatch
	// artifact.Base provides an empty implementation
	Mapper() func() Mapper

	DiscoverArtifacts(ctx context.Context) error

	DownloadArtifact(context.Context, *types.ArtifactInfo) error
}

/*
An extractor takes input an generates output

Possible inputs:
- a file path (which may be a local copy of a downloaded file)
- an (unknown) artifact in the form of a go interface{}
- a stream of bytes (???
Possible outputs:
- a deserialized object (passed in an ArtifactExtracted event)
- a stream of Row events
- a stream of bytes (???)

Perhaps 3 types of extractor
- extractor source (takes a file location and generates intermediate data (interface{})
- extractor mapper (takes intermediate data and generates intermediate data )
- extractor sink (takes intermediate data and generates rows)
NOTE: an extractor may could be source AND sink, i.e. a single extraction stage

QUESTIONS
how do we advertise/check the properties of each extractor
- and how do we verify that a given extractor chain is valid

Eg for CloudTrail local file gzipped logs

	Artifact source: FileSystemSource
	Artifact extractors: GzipExtractorSource, CloudTrailExtractor

Eg for CloudTrail s3 bucket gzipped logs

	Artifact source: S3BucketArtifactSource
	Artifact extractors: GzipExtractorSource, CloudTrailExtractor


Eg for VPC FlowLog local file gzipped logs

	Artifact source: FileSystemSource
	Artifact extractors: GzipExtractorSource, FlowLogExtractor

Eg for CloudTrail s3 bucket gzipped logs

	Artifact source: S3BucketArtifactSource
	Artifact extractors: GzipExtractorSource, FlowLogExtractor

*/

// Loader is an interface which provides a method for loading a locally saved artifact
// an [row_source.ArtifactRowSource] must be configured to have a Loader implementation.
// Sources provided by the SDK: [GzipLoader], [GzipRowLoader], [FileSystemLoader], [FileSystemRowLoader]
type Loader interface {
	Identifier() string
	// Load locally saved artifact data and perform any necessary decompression/decryption
	Load(context.Context, *types.ArtifactInfo, chan *ArtifactData) error
}

// Mapper is an interface which provides a method for mapping artifact data to a different format
// an [row_source.ArtifactRowSource] may be configured to have one or more Mappers.
// Mappers provided by the SDK: [CloudwatchMapper]
type Mapper interface {
	Identifier() string
	// Map converts artifact data to a different format and either return it as rows,
	// or pass it on to the next mapper in the chain
	Map(context.Context, *ArtifactData) ([]*ArtifactData, error)
}
