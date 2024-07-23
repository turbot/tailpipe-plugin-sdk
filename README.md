# tailpipe-plugin-sdk

### 1 Overview
Tailpipe consists of a CLI and an ecosystem of GRPC plugins, similar to Steampipe.
CLI:
- configure a collection in a HCL config file
- execute CLI `collect`
- CLI starts the required GRPC plugin and calls `AddObserver` to receive plugin status messages 
- CLI issues a `Collect` command to the plugin.
- Plugin writes chunks of data to JSONL files in the specified output directory, and send a `Chunk` event for each file
- The CLI handles the `Chunk` event by loading the JSONL file and converting it to partitioned parquet files in the configured parquet location
- (In future) the CLI will automatically generate DuckDB views  on this data

Plugin Collection Process:
  - Plugin receives a `Collect` command
  - Plugin instantiates the configured `Collection` which in turn instantiates the configured `Source`
  - The `Source` retrieves rows of data from the source location and raises `Row` events which are handled by the `Collection`
  - The `Collection` enriches the rows and raises `Row` events which are handled by the plugin sdk, which buffers the rows 
  - When the row buffer is full the plugin sdk writes the rows to a JSONL file in the specified output directory, and raises a `Chunk` event back across the GRPC event stream

Plugins are composed of  the following components:
- Plugin constructor
- Collections
- Sources [optional]
- Mappers [optional]
- Loader [optional]

The plugin must implement at least one collection.
It can optionally define sources,source mappers or source loaders if required,
or it can use the built in sources/mappers/loader provided by the sdk (see below),

Examples:
- [AWS](https://github.com/turbot/tailpipe-plugin-aws/tree/development)
- [Pipes](https://github.com/turbot/tailpipe-plugin-pipes/tree/development)


## 2 Implementing a plugin
### 2.1 tl;dr
Steps to implement a plugin:
  - Implement a plugin struct which embeds `plugin.Base`. 
  - Add a constructor function for this and call plugin.Serve from main, passing this constructor.

### 2.2 Plugin Interface
The plugin should define a `Plugin` struct which implements the [TailpipePlugin interface](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/plugin/interfaces.go):
```go
type TailpipePlugin interface {
	// Identifier returns the plugin name
	// this must be implemented by the plugin implementation
	Identifier() string

	// GetSchema returns the duck DB schema for all collections
	// this must be implemented by the plugin implementation
	GetSchema() schema.SchemaMap

	// AddObserver adda an observer to the plugin to receive status events
	// this is implemented by plugin.Base and should not be overridden
	AddObserver(observable.Observer) error

	// Collect is called to start a collection run
	// this is implemented by plugin.Base and should not be overridden
	Collect(context.Context, *proto.CollectRequest) error

	
	// Other interface functions

	// Init is implemented by plugin.Base.
	// If overridden by the plugin it MUST call the base version
	Init(context.Context) error

	// Shutdown is implemented by plugin.Base (empty implementation)
	// it may be overridden by the plugin
	Shutdown(context.Context) error
}
```

The `Plugin` struct should embed the [plugin.Base](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/plugin/base.go) struct, which provides a default implementation of the [Observable](https://github.com/turbot/tailpipe-plugin-sdk/blob/114315fb39ed91e1f0b83f78d6f3aff4425c12d7/observable/observable.go#L8) interface,
and a lot of the underlying functionality of the plugin.


#### 2.2.1 Functions which must be implemented when writing a new plugin
- `Identifier` - return the plugin name
- `GetSchema` - return the schema for all collections

Optionally, the plugin may implement the following functions:
- `Init` - any initialisation required by the plugin. Note: if this is implemented it must call `Base.Init()`.
- `Shutdown` - any cleanup required by the plugin. Note: if this is implemented it must call `Base.Shutdown()`.

Example:
- [AWS](https://github.com/turbot/tailpipe-plugin-aws/blob/48522882f125adbb8c6e4e2577455c5b9006dc98/aws/plugin.go#L13)
 

### 2.2 Folder conventions
the plugin folder structure should be:
``` 
main.go
<plugin_name>/
  plugin.go
<plugin_name>_collection/
  <collection_name>_collection.go
  <collection_name>_collection_config.go
<plugin_name>_source/
  <source_name>_source.go*
  <source_name>_collection_config.go*
  <source_name>_mapper.go*
<plugin_name>_types/
  <plugin_name>_<rowdata_type>.go

``` 
Notes:
- Files marked with an asterisk (*) are optional.
- *<plugin_name>* is a placeholder for the name of the plugin.
- *<collection_name>* is a placeholder for the name of the collection. There will be one or more collections.
- *<source_name>* is a placeholder for the name of the source. There will be zero or more sources defined.


For example AWS: 
```
main.go
aws/
  plugin.go
aws_collection/
  cloudtrail_log_collection.go
  cloudtrail_log_collection_config.go
  vpc_flow_log_collection.go
  vpc_flow_log_collection_config.go
aws_source/
  cloudtrail_mapper.go
aws_types/
  aws_cloudtrail.go
  vpc_flow_log.go
  vpc_flow_log_test.go
```

### Plugin constructor
The plugin must implement a constructor function. This should be in the file `<plugin_name>/ plugin.go`. The plugin constructor must:
  - instantiate a `Plugin` struct
  - register the collections which the plugin provides by calling `RegisterCollections` on the plugin struct (this is a method provided by the `plugin.Base` struct).
  - return the  `Plugin` object

Example:
- [AWS](https://github.com/turbot/tailpipe-plugin-aws/blob/48522882f125adbb8c6e4e2577455c5b9006dc98/aws/plugin.go#L17)

In the main function of the plugin, call `plugin.Serve` with the plugin constructor function as an argument.
for example from [aws](https://github.com/turbot/tailpipe-plugin-aws/blob/882f2b64d99d690842759761e90dcf64e6a236e8/main.go)
```go
func main() {
	err := plugin.Serve(&plugin.ServeOpts{
		PluginFunc: aws.NewPlugin,
	})

	if err != nil {
		slog.Error("Error starting plugin", "error", err)
	}
}
```

### 2.3 Collections

A `Collection` is broadly analogous to a `table` in steampipe. It returns a set of data which follows a specific schema. 
This schema will have a number of standard fields (see `GetRowSchema` below) and may have additional fields which are specific to the collection.

A plugin must define at least one collection, and may define more. 

#### 2.3.1 Collection Interface
The collection must implement the [plugin.Collection interface](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/plugin/interfaces.go):


```go
type Collection interface {
	// Observable must be implemented by collections (it is implemented by collection.Base)
	observable.Observable

	// Init is called when the collection created 
	// it is responsible for parsing the config and creating the configured Source 
	Init(ctx context.Context, config []byte) error
	// Identifier must return the collection name
	Identifier() string
	// GetRowSchema returns an empty instance of the row struct returned by the collection
	GetRowSchema()any
	// GetConfigStruct returns an empty instance of the config struct returned by the collection
	GetConfigSchema()any
	// GetPagingDataStruct returns an empty instance of the paging data struct 
	// Should be implemented only if paging is supported (Base bas an empty implementation) 
	GetPagingDataSchema()(paging.Data, error)

	// Collect is called to start collecting data,
	// Collect will send enriched rows which satisfy the tailpipe row requirements (todo link/document)
	Collect(context.Context, *proto.CollectRequest) error
	
	// EnrichRow is called for each raw row of data, it must enrich the row and return it
	EnrichRow(row any, sourceEnrichmentFields *enrichment.CommonFields) (any, error)
}
```

#### Base class
All collection implementations should embed the [collection.Base](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/collection/base.go) struct, which provides a default implementation of the [Observable](https://github.com/turbot/tailpipe-plugin-sdk/blob/114315fb39ed91e1f0b83f78d6f3aff4425c12d7/observable/observable.go#L8) interface.
It also implements the [Collect](https://github.com/turbot/tailpipe-plugin-sdk/blob/114315fb39ed91e1f0b83f78d6f3aff4425c12d7/collection/base.go#L37) function and provides a default implementation of `GetPagingDataStruct`. 

#### Interface functions which must be implemented when writing a plugin

- `Init`
  - Parse the config  (using the `base.ParseConfig` function)
  - Create the configured `Source`
  - Any other collection specific intialisation
 
- Identifier
  - Return the collection name

- GetRowSchema
This specifies the row schema that the collection will return. This should return an empty instance of the struct that the collection will return.
  
#### Defining the Row Struct
The row struct are the types returned by the collection, and they define the collection schema.

The struct definitions should be in the folder `<plugin_name>_types/` in files named `<plugin_name>_<rowdata_type>.go`. 

All fields shoul dhave json tags 
##### standard fields
These row struct must include the following JSON tagged fields:
- `tp_connection`
- `tp_year`
- `tp_month`
- `tp_day`
- `tp_id`
- `tp_timestamp`

The following optional enrichment fields may also be added.
- `tp_source_type`
- `tp_source_name`
- `tp_source_location`
- `tp_ingest_timestamp`
- `tp_source_ip`
- `tp_destination_ip`
- `tp_collection`
- `tp_akas`
- `tp_ips`
- `tp_tags`
- `tp_domains`
- `tp_emails`
- `tp_usernames`

The row struct returned by `GetRowSchema` should be embed [`enrichment.CommonFields`](https://github.com/turbot/tailpipe-plugin-sdk/blob/114315fb39ed91e1f0b83f78d6f3aff4425c12d7/enrichment/common_fields.go) to include these fields.

##### Customising the schema


## Technical Overview 
### Plugin GRPC Interface

#### AddObserver
Returns a stream used by the plugin to send status/progress events


#### Collect
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



#### API Row Source
For log sources that are accessed via an API, the plugin may define a custom which has specific 
knowledge of the API and credentials and directly fetches log items from the API.

The source would be responsible for:
- managing the credentials (using the configured connection)
- maintaining state of the most recent log item fetched so only new items are fetched
- applying source filtering of fetched items as specified by the collection/source config
- streaming the log items to the collection for enrichment




## Collection