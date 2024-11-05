// Package artifact_row_source provides types and functions for loading and processing rows using an
// [row_source.RowSourceImpl].
//
// Artifacts are defined as some entity which contains a collection of rows, which must be extracted/processed in
// some way to produce 'raw' rows which can be streamed to a collection. Examples of artifacts include:
// - a gzip file in an S3 bucket
// - a cloudwatch log group
// - a json file on local file system
//
// An RowSourceImpl is composed of:
// - an [artifact.ArtifactSource] which discovers and downloads artifacts to a temp local file, and handles incremental/restartable downloads
// - an [artifact.Loader] which loads the arifact data from the local file, performing any necessary decompression/decryption etc.
// - optionally, one or more [artifact.Mapper]s which perform processing/conversion/extraction logic required to
//
// Sources provided by the SDK:
// - [FileSystemSource]
// - [AwsS3BucketSource]
// - [AwsCloudWatchSource]
//
// Loaders provided by the SDK:
// - [GzipLoader]
// - [GzipRowLoader]
// - [FileSystemLoader]
// - [FileSystemRowLoader]
//
// Mapper provided by the SDK
// - [artifact.CloudwatchMapper](https:// github.com/turbot/tailpipe-plugin-sdk/blob/development/artifact/aws_cloudwatch_mapper.go)
//
// ##### Artifact extraction flow
//
// - The source discovers artifacts and raises an ArtifactDiscovered event, which is handled by the parent RowSourceImpl.
// - The RowSourceImpl initiates the download of the artifact by calling the source's `Download` method. RowSourceImpl is responsible for managing rate limiting/parallelization
// - The source downloads the artifact and raises an ArtifactDownloaded event, which is handled by the parent RowSourceImpl.
// - The RowSourceImpl tells the loader to load the artifact, passing an `ArtifactInfo` containing the local file path.
// - The loader loads the artifact and performs and processing it needs to and returns the result
// - If any mappers are configured, they are called in turn, passing the result along
// - The final result is published in a `Row` event.
//
// _Note: a mapper is not always necessary - sometimes the output of the loader will be raw rows.
// An example of this is when FlowLog collection uses the GzipExtractorSource, which simply unzips the artifact,
// splits it into texting and passes the raw rows to the collection.
//
// Examples:
//
// **CloudTrail local file gzipped logs**
//
// - source: FileSystemSource
// - loader:  GzipExtractorSource
// - mapper:  CloudTrailMapper
//
// **CloudTrail s3 bucket gzipped logs**
//
// - source: S3BucketArtifactSource
// - loader: GzipLoader
// - mapper: CloudTrailMapper
//
// **VPC FlowLog local file gzipped logs**
//
// - source: FileSystemSource
// - loader: GzipRowLoader

package artifact_source
