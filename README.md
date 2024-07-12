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
Responsible for locating and downloading the artifact from storage. THe artifact is downloaded to a temp local location. 

Examples: 
- `artifact.FileSystemSource`
- `artifact.S3BucketSource`
- `artifact.CloudwatchSource`

##### Artifact loader
Responsible for loading the locally downloaded artifact and potentially performing some initial processing on it.


##### Artifact extractor
Responsible for performing additional processing on the loaded artifact to extract the log rows. (note - several extractors may be chained together)


##### Artifact extraction flow

- The source discovers artifacts and raises an ArtifactDiscovered event, which is handled by the parent ArtifactRowSource.
- The ArtifactRowSource determines whether the artifact has already been downloaded (TODO - maintain a state file of downloaded artifacts). 
If not it tells the source to download it.  
- The source downloads the artifact and raises an ArtifactDownloaded event, which is handled by the parent ArtifactRowSource.
- The ArtifactRowSource tells the loader to load the artifact, passing an `ArtifactInfo` containing the local file path.
- The loader loads the artifact and performs and processing it needs to and returns the result
- If any mappers are configured, they are called in turn, passing the result along
- The final result is published in a `Row` event.


_Note: a mapper is not always necessary - sometimes the output of the loader will be raw rows. 
An example of this is when FlowLog collection uses the GzipExtractorSource, which simply unzips the artifact, 
splits it into texting and passes the raw rows to the collection._ 


Eg for CloudTrail local file gzipped logs

	Artifact source: FileSystemSource
	Artifact loader:  GzipExtractorSource (loader)
      CloudTrailExtractor (mapper

Eg for CloudTrail s3 bucket gzipped logs

	Artifact source: S3BucketArtifactSource
	Artifact extractors: 
      GzipExtractorSource (loader)
      CloudTrailExtractor (mapper)


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