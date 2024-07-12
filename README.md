# tailpipe-plugin-sdk

## Plugin GRPC Interface

### AddObserver
Returns a stream used by the plugin to send status/progress events


## Collect
- Tell the plugin to start collection

- NOTE: The plugin will execute collection asyncronously, i.e. this call will return immediately and the collection status
  will be updated via the event streeam returned from `AddObserver`

- The plugin sends an event on start and completions (progress events tbd)
- The plugin will rows in chunks to JSONL files in the specified directory. The filename will start with the execution id and end with a sequence number
- the complete event will contain the number of files written - then when collection is complete the plugin manager will
  ensure it has processed all files _for that execution id_)  



# Components
## Source

A `Source` is responsible for retrieving raw rows from their source locaiton, and streaming them to the collection for enrichment.

The source is responsible for a combination of the following tasks:
- Locating artifacts containing log data (e.g. gz files in an S3 bucket)
- Downloading the artifact from storage (local/remote/cloud)
- Extracting the raw log rows from the artifact (this may involve extraction/mapping of the log format/location)
- Retrieving log rows from an API
- Keeping track of which log rows have been downloaded and only downloading new ones 

### Events Raised
#### Row
Stream a raw log row to the collection for enrichment

The format of the raw row must be expected/supported by the collection.



### Events Handled
n/a

### Interface
```go
type RowSource interface {
	// Observable must be implemented by row sources (it is implemented by row_source.Base)
	observable.Observable

    // clear all cached data, close any connections etc
	Close()error

	// Collect is called to start collecting data from the source
	Collect(context.Context, *proto.CollectRequest) error
}
```

### Types of sources
#### Artifact Row Source
An artifact row source retrieves log data from some kind of artifact, such as a file in a local or remote file system, or an object in an object store.

The artifact row source is composable, as the same storage lcoaiton may be used to store differnet log files in varying formats, 
and the source may need to be configured to know how to extract the log rows from the artifact.

The artifact row source is split into two parts:
##### Artifact source
Responsible for locating and downloading the artifact from storage. 

Examples: 
- `artifact.FileSystemSource`
- `artifact.S3BucketSource`
- `artifact.CloudwatchSource`

##### Artifact extractor
Responsible for extracting the log rows from the artifact

An extractor takes input and generates output

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


The artifact extractor is itself composable. An artifact extractor consists of:
- A `source`



#### API ROw Source
For log sources that are accessed via an API, the plugin may define a custom which has specific 
knowledge of the API and credentials and directly fetches log items from the API.

The source would be responsible for:
- managing the credentials (using the configured connection)
- maintaining state of the most recent log item fetched so only new items are fetched
- applying source filtering of fetched items as specified by the collection/source config
- streaming the log items to the collection for enrichment


#### Webhook Source



## Collection