# Collection State

 # BRANCH
coll_stater taken from source_plugin+rebase
## NEW

- Collection state knows how to load and save itself - is is given a location by the plugin and is responsible for 
  loading and saving itself from that location.
  - It is loaded at the start of collection
  - It is saved every time the plugin sends a chunksConverted message - this contains the highest contiguous chunk
    that has been converted.

## Regions
The collection state is a set of regions. Each region has:
- a start and end time
- start and end granularity (usually the same unless collecting the most recent data, in which case the end granularity
is the 'error'/'jitter' of the source. I.e. if we are only certain that the data id complete up to 30 minutes ago, 
the end granularity is 30 mins, i.e. eny daya from the last 30 mins goes in the end objects map.   
- start and end objects - these are the ids/paths of objects collected within the granularity period 

## Incremental collection state
For each chunk that is written, the collection state stores the incremental state of the collection for that chunk. 
This is the new end time (or start time if collecting backwards), as well as the end objects associated with that chunk.

When a chunksConverted message is received, the incremental collection states are applied in order as follows:
For each chunk
- If the end time < the current end time end time, do nothing (the end objects are no longer valid)
- If the end time == the filename end time, add end objects to the end objects map
- If the end time > the filename end time, update the end time and REPLACE the objects map with the new objects map


Q: How do we ghenerate collection state per chunk????
Q: Do we/how do we collect artifacts in time order???

## OLD

## Overview

CollectionState is uses to determine what data has already been loaded.

The details of the data required to make this choice will be dependent on the RowSource type, as well as the format of the 
data being stored, which may depend on the Collection type, and also may be configurable in the SourceConfig

By default, all ArtifactRowSources will use `ArtifactCollectionState`, which uses a regex to extract timing information 
from the filename of the artifact. It is the responsible of the collection to pass the correct regex to the RowSource 
- either from the source/table config, or providing a default appreopriate to the colleciton type.

There is already a mechanism for the colleciton to specify options to pass to the RowSource - add WithCollectStateOptions
to the RowSource options.



## Lifecycle and Schema

`RowSource` interface includes the following paging functions

//// SetPager sets the pager for the source
//SetPager(pager paging.Pager)

`GetCollectionStateSchema` returns an empty instance of the collection state data struct. This Should be implemented only if paging is supported (RowSourceBase has an empty implementation)
`GetCollectionStateJSON` returns the json serialised collection state data for the ongoing collection
GetCollectionStateJSON() (json.RawMessage, error)
`SetCollectionStateJSON` unmarshalls the collection state data JSON into the target object


Lifecycle

The RowSource is created by the Collection (in `CollectionBase`). Collection imnplementations can provide options which 
must be used when creating their RowSource - this is how the Collection can configure the RowSource.




Each RowSource implementation must register itself (specifically a ctro to creat an empty source) with the RowSource factory
 - this should be done from an `init` function
e.g. for AwsClodwatchSource:
```go
func init() {
	// register source
	row_source.Factory.RegisterRowSources(NewAwsCloudWatchSource)
}
```


`RowSourceFactory.GetRowSource` instantiates the source using the registerd constructor. It then calls source.Init() 

	// if the source is an artifact source, we need a mapper
		// NOTE: WithMapper option will ONLY apply if the RowSource IS an ArtifactSource
		artifact_source.WithMapper(aws_source.NewCloudtrailMapper(),
		row_source.WithCollectionState(aws_source.NewCloudtrailPager)),

artifact_source.WithRowPerLine(),
artifact_source.WithMapper(aws_source.NewS3ServerAccessLogMapper()),