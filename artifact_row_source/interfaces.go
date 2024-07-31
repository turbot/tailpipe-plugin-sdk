package artifact_row_source

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/hcl"
)

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

type ArtifactSourceFactory interface {
	// GetArtifactSource attempts to instantiate an artifact source, using the provided data
	// It will fail if the requested source type is not registered
	// Implements [plugin.SourceFactory]
	GetArtifactSource(context.Context, *hcl.Data) (artifact_source.Source, error)
}
