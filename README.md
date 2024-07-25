# tailpipe-plugin-sdk

## 1 Implementing a plugin

### 1.1. Tailpipe Overview 

Tailpipe consists of a CLI and an ecosystem of GRPC plugins, similar to Steampipe.

Overview of Tailpipe operation:
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

Plugins are composed of the following components:
- Plugin struct (a minimal shell)
- Collections
- Sources 

The plugin must implement at least one collection.
It can optionally define sources to retrieve data, or it can use the build in sources provided by the SDK. 

Examples:
- [AWS](https://github.com/turbot/tailpipe-plugin-aws/tree/development)
- [Pipes](https://github.com/turbot/tailpipe-plugin-pipes/tree/development)


### 1.2 Implementation Steps
  #### 1.2.1 Implement plugin struct
  - Implement a plugin struct which embeds `plugin.Base`, and implements the `Identifier` function
  - Add a constructor function for this and call `plugin.Serve` from the main function, passing this constructor in the `ServeOpts`.
  #### 1.2.2 Define one or more Collections
  - Implement a collection struct which embeds `collection.Base` 
  - Implements the required Collection interface functions:
    - `Identifier`
    - `GetRowSchema`
    - `GetConfigSchema`
    - `GetPagingDataSchema` (optional)  
    - `EnrichRow` 
  - Define a row struct row the collection will return - this defines the schema of the collection and should embed `enrichment.CommonFields` 
  - Define a config struct with HCL tags for the collection config
  - [TODO] Define/register which sources the collection supports
  - Register the collection with the plugin by calling `RegisterCollection` on the plugin struct within the plugin constructor. 
#### 1.2.3 [optional] Define custom Sources 
  - Implement a source struct which embeds `row_source.Base` 
  - Implement the required RowSource interface functions:
      - `Identifier`
      - `Collect`
  - [TODO] resister the source with the collection/plugin

The `Collect` function should retrieve row and (optionally) enrichment data and for each row retrieved 
create an `ArtifactData` struct and raise a row event by calling the `OnRow` method, implemented by the `row_source.Base` struct. 

## 2 Details

### 2.1 Plugin

#### 2.1.1 TailpipePlugin Interface
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


#####  Functions which must be implemented when writing a new plugin
- `Identifier` - return the plugin name
- `GetSchema` - return the schema for all collections

Optionally, the plugin may implement the following functions:
- `Init` - any initialisation required by the plugin. Note: if this is implemented it must call `Base.Init()`.
- `Shutdown` - any cleanup required by the plugin. Note: if this is implemented it must call `Base.Shutdown()`.

Example:
- [AWS](https://github.com/turbot/tailpipe-plugin-aws/blob/48522882f125adbb8c6e4e2577455c5b9006dc98/aws/plugin.go#L13)

#### 2.1.2 Plugin constructor
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

### 2.3 Collections

A `Collection` is broadly analogous to a `table` in steampipe. It returns a set of data which follows a specific schema. 
This schema will have a number of standard fields (see `GetRowSchema` below) as well as fields specific to the collection.

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

##### Base class
All collection implementations should embed the [collection.Base](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/collection/base.go) struct, which provides a default implementation of the [Observable](https://github.com/turbot/tailpipe-plugin-sdk/blob/114315fb39ed91e1f0b83f78d6f3aff4425c12d7/observable/observable.go#L8) interface.
It also implements the [Collect](https://github.com/turbot/tailpipe-plugin-sdk/blob/114315fb39ed91e1f0b83f78d6f3aff4425c12d7/collection/base.go#L37) function and provides a default implementation of `GetPagingDataStruct`. 

##### Interface functions which must be implemented when defining a collection

- `Init` (MAYBE???)
  - Parse the config  (using the `base.ParseConfig` function)
  - Create the configured `Source`
  - Any other collection specific intialisation

- `Identifier`
  - Return the collection name
- `GetRowSchema`
  - This specifies the row schema that the collection will return. This should return an empty instance of the struct that the collection will return.
- `GetConfigSchema`
  - This specifies the schema of the config that the collection expects. This should return an empty instance of the struct that the collection expects. The struct should have HCL tags for the config fields.
- `GetPagingDataSchema` (optional)
  - This specifies the schema of the paging data that the collection expects. This should return an empty instance of the struct that the collection expects.
  
#### Defining the Row Struct
The 'row struct' is the type returned by the collection, and they define the collection schema.

The struct definitions should be in the folder `<plugin_name>_types/` in files named `<plugin_name>_<rowdata_type>.go`. All fields should have json tags

The row struct should embed [enrichment.CommonFields](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/enrichment/common_fields.go) to include a set of standard Tailpipe fields.

##### Customising the Collection Schema
By default, the collection schema is inferred automatically be reflecting the row struct. all field names are converted to snake case and the field types are converted to the matching DuckDb types.

If the schema of particular fields needs to be customised, a `parquet` tag can be added to the field. This tag should contain the duckDB type of the field, and/or desired field name (it is possible to provide just one of these fields).

For example:
```go
type MyRow struct {
    enrichment.CommonFields

    // override type
    Id string `json:"id" parquet:"type=UUID"`
    // override name
    MyField string `json:"my_field" parquet:"name=description"`
    // exclude from schema
    Exclude string `json:exclude" parquet:"-"`
}
```

#### Enriching the row
The primary function of the collection is to enrich/normalise the raw data returned by the source, returning a standardised row struct.

This is achieved by the `EnrichRow` function. This function is called for each raw row of data returned by the source.
It is expected that the collection will know (oe be able to deduce) what the format of the source data. 

(This may be achieved by the plugin implementing a custom [Mapper](https://github.com/turbot/tailpipe-plugin-sdk/blob/1216d91b3c6d009933878861fa8b79cb8086e0e1/artifact/interfaces.go#L76) to perform the final stage of data conversion for the source.
For example, the AWS plugin uses a [CloudTrailMapper](https://github.com/turbot/tailpipe-plugin-aws/blob/development/aws_source/cloudtrail_mapper.go) to convert JSON data from CloudTrail into an `AWSCloudTrailBatch` and then extracts thge rows from this.)

The `EnrichRow` function should create an instance of the row struct, populate it with the data from the raw row, and populate whichever of the standard Tailpipe fields are available/relevant.

##### Standard Tailpipe Fields
The standard Tailpipe fields are contained in the  [enrichment.CommonFields](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/enrichment/common_fields.go) struct which must be embedded into the row struct.
The following standard fields MUST be populated in the row struct:

- `TpID`
- `TpConnection`
- `TpTimestamp`
- `TpYear`
- `TpMonth`
- `TpDay`

The following optional enrichment fields may also be added.

- `TpSourceType`
- `TpSourceName`
- `TpSourceLocation`
- `TpIngestTimestamp`
- `TpSourceIP`
- `TpDestinationIP`
- `TpCollection`
- `TpAkas`
- `TpIps`
- `TpTags`
- `TpDomains`
- `TpEmails`
- `TpUsernames`

## 2.4 Sources

A `Source` is responsible for retrieving raw rows from their source location, and streaming them to the collection for enrichment.

The source is responsible for a combination of the following tasks:
- Locating artifacts containing log data (e.g. gz files in an S3 bucket)
- Downloading the artifact from storage (local/remote/cloud)
- Extracting the raw log rows from the artifact (this may involve extraction/mapping of the log format/location)
- Retrieving log rows from an API
- Keeping track of which log rows have been downloaded and only downloading new ones

#### 2.4.1 RowSource Interface
The source must implement the [plugin.RowSource interface](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/plugin/interfaces.go):

```go
// RowSource is the interface that represents a data source
// A number of data sources are provided by the SDK, and plugins may provide their own
// Built in data sources:
// - AWS S3 Bucket
// - API Source (this must be implemented by the plugin)
// - File Source
// - Webhook source
// Sources may be configured with data transfo
type RowSource interface {
	// Observable must be implemented by row sources (it is implemented by row_source.Base)
	observable.Observable

	Close() error
	Collect(context.Context) error
}
```

##### Base class
All RowSource implementations should embed the [row_source.Base](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/row_source/base.go) struct, which provides a default implementation of the [Observable](https://github.com/turbot/tailpipe-plugin-sdk/blob/114315fb39ed91e1f0b83f78d6f3aff4425c12d7/observable/observable.go#L8) interface.
It also implements the `Close` function and implements raising `Row` events with the `OnRow` function. 


##### Interface functions which must be implemented when defining a collection

- `Collect`
  - retrieve row and (optionally) enrichment data and for each row retrieved create an `ArtifactData` struct and raise a row event by calling the `OnRow` method, implemented by the `row_source.Base` struct.  

### 2.4.2 ArtifactRowSource

[ArtifactRowSource](https://github.com/turbot/tailpipe-plugin-sdk/blob/ca406c79bcbc9249f8b75c0ade1c50a90021eb83/row_source/artifact_row_source.go) is a [RowSource](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/plugin/interfaces.go) implementation provided by the SDK. 
It is used to retrieve log data from some kind of artifact, such as a file in a local or remote file system, or an object in an object store.

The ArtifactRowSource is composable, as the same storage location may be used to store different log files in varying formats,
and the source may need to be configured to know how to extract the log rows from the artifact.

The ArtifactRowSource is split into 3 parts
##### Artifact source
Responsible for locating and downloading the artifact from storage. The artifact is downloaded to a temp local location.

Artifact sources provided by the SDK:
- [artifact.FileSystemSource](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/artifact/file_system_source.go)
- [artifact.AwsS3BucketSource](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/artifact/aws_s3_bucket_source.go)
- [artifact.AwsCloudWatchSource](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/artifact/aws_cloudwatch_source.go)

##### Artifact loader
Responsible for loading the locally downloaded artifact and potentially performing some initial processing on it.
Artifact loaders provided by the SDK:
- [artifact.GzipLoader]https://github.com/turbot/tailpipe-plugin-sdk/blob/development/artifact/gzip_loader.go
- [artifact.GzipRowLoader]https://github.com/turbot/tailpipe-plugin-sdk/blob/development/artifact/gzip_row_loader.go
- [artifact.FileSystemLoader]https://github.com/turbot/tailpipe-plugin-sdk/blob/development/artifact/file_system_loader.go
- [artifact.FileSystemRowLoader]https://github.com/turbot/tailpipe-plugin-sdk/blob/development/artifact/file_system_row_loader.go

##### Artifact mapper
Responsible for performing additional processing on the loaded artifact to extract the log rows. (note - several mappers may be chained together)
Mappers provided by the SDK
- [artifact.CloudwatchMapper](https://github.com/turbot/tailpipe-plugin-sdk/blob/development/artifact/aws_cloudwatch_mapper.go)

##### Artifact extraction flow

- The source discovers artifacts and raises an ArtifactDiscovered event, which is handled by the parent ArtifactRowSource.
- The ArtifactRowSource initiates the download of the artifact by calling the source's `Download` method. ArtifactRowSource is responsible for managing rate limiting/parallelization
- The source downloads the artifact and raises an ArtifactDownloaded event, which is handled by the parent ArtifactRowSource.
- The ArtifactRowSource tells the loader to load the artifact, passing an `ArtifactInfo` containing the local file path.
- The loader loads the artifact and performs and processing it needs to and returns the result
- If any mappers are configured, they are called in turn, passing the result along
- The final result is published in a `Row` event.


_Note: a mapper is not always necessary - sometimes the output of the loader will be raw rows.
An example of this is when FlowLog collection uses the GzipExtractorSource, which simply unzips the artifact,
splits it into texting and passes the raw rows to the collection.


Examples:

**CloudTrail local file gzipped logs**

- source: FileSystemSource
- loader:  GzipExtractorSource 
- mapper:  CloudTrailMapper 

**CloudTrail s3 bucket gzipped logs**

- source: S3BucketArtifactSource
- loader: GzipLoader 
- mapper: CloudTrailMapper 

**VPC FlowLog local file gzipped logs**

- source: FileSystemSource
- loader: GzipRowLoader




### 2.4.3 Custom Row Sources
For log sources that are accessed via an API, the plugin may define a custom which has specific
knowledge of the API and credentials and directly fetches log items from the API.

The source would be responsible for:
- managing the credentials (using the configured connection)
- maintaining state of the most recent log item fetched so only new items are fetched
- applying source filtering of fetched items as specified by the collection/source config
- streaming the log items to the collection for enrichment

## 3 Technical Details 
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

