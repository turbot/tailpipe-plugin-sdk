package artifact

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Source interface {
	observable.Observable
	Close() error
	DiscoverArtifacts(context.Context, *proto.CollectRequest) error
	DownloadArtifact(context.Context, *proto.CollectRequest, *types.ArtifactInfo) error
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

type Extractor interface {
	observable.Observable
	observable.Observer

	// functions which report whether this extractor is a source/mapper/sink

	//IsSource() bool
	//IsMapper() bool
	//IsSink() bool
}

type ExtractorSource interface {
	// ExtractArtifact loads artifact data and pass it on to the next extractor in the chain
	ExtractArtifact(context.Context, *proto.CollectRequest, *types.ArtifactInfo) error
}

type ExtractorMapper interface {
	// MapArtifact converts artifact data to a different format and pass it on to the next extractor in the chain
	MapArtifact(context.Context, *proto.CollectRequest, *types.Artifact) error
}

type ExtractorSink interface {
	// ExtractArtifactRows converts artifact data to a set of rows
	ExtractArtifactRows(context.Context, *proto.CollectRequest, *types.Artifact) error
}
