## v0.9.5 [2026-07-10]
_Dependencies_
* Bump `pipe-fittings` to v2.9.3, removing the EOL `containerd` dependency and clearing govulncheck advisories GO-2026-5622, GO-2026-5338, and GO-2026-5064.
* Bump `go-getter` to 1.7.9, `xz` to 0.5.14, and `mapstructure` to 2.4.0.

## v0.9.4 [2025-09-26]
_Whats new_
* Add support for Azure Blob Storage as an artifact source.

## v0.9.3 [2025-09-11]
_Whats new_
* ConversionSchema updated to sort columns and exposers ColumnStrings property.
* Add function SortColumnsAlphabetically. 

## v0.9.2 [2025-07-24]
_Bug fixes_
* Fix issue where a collection for zero-granularity data was missing collection boundaries which was causing incorrect collection ranges. ([#264](https://github.com/turbot/tailpipe-plugin-sdk/issues/264))
* Fix the crash when a collection state had null trunk states. ([#261](https://github.com/turbot/tailpipe-plugin-sdk/issues/261))

_Dependencies_
* Upgrade `golang.org/x/oauth2` to remediate high vulnerabilities. 

## v0.9.1 [2025-07-02]
_Bug fixes_
* Do not clear end objects in TimeRangeCollectionState.OnCollectionComplete if granularity is zero. ([#251](https://github.com/turbot/tailpipe-plugin-sdk/issues/251))
* TimeRangeObjectState.Validate does not validate TimeRange if granularity is zero

## v0.9.0 [2025-07-02]
_Whats new_

- Refactor collection state to support time ranges, enabling `--to` flag support. ([#241](https://github.com/turbot/tailpipe-plugin-sdk/issues/241)) 
  - TimeRangeCollectionState supports array of time ranges
  - ShouldCollect ensures that the gaps between the ranges are filled but we do not collect for times we have already collected
  - The time ranges, of type DirectionalTimeRange, are direction aware and work for collection in forwards or backwards direction
  - A Clear function allows clearing the state for a specified time range
  - Move all persistence logic into SaveableCollectionState
  - Added `overwrite` parameter to CollectRequest - if set, clear collection state becore collecting
  - Added migration support for legacy collection states

_Bug fixes_
* Fix issue where collection state end-objects are cleared when collection is complete,
  meaning no further data will be collected for that day. ([#250](https://github.com/turbot/tailpipe-plugin-sdk/issues/250))


## v0.8.0 [2025-06-23]
_Whats new_
* Remove row validation and rely entirely on CLI to execute validation. ([#202](https://github.com/turbot/tailpipe-plugin-sdk/issues/202))
  * Remove row validation
  * Remove `RowStruct` interface and use any for type constraint for Mapper etc. instead
  * Remove `row.Validate` call from `handleRowExtractedEvent`
  * Remove `DynamicRow.Validate`
* Update ArtifactConversionCollector to support for Pre-Defined Custom Tables .([#230](https://github.com/turbot/tailpipe-plugin-sdk/issues/230))
  * `executeConversionQuery` gets schema using CustomTable.GetCustomSchema instead of reading schema from request
  * `executeConversionQuery` now performs column transforms instead of cli
  * `TableSchema.WithSourceFieldsCleared` also clears transforms and struct fields
  *  `getCopyQuery` skips columns missing in source
  * Update `ColumnSchema.Clone` to clone struct fields
  * `CustomTableImpl.Initialize` now calls validate and returns error
  * `TableSchema.Validate` now validates column types
  * Add `GetCustomSchema` to `CustomTable` interface to return the schema excluding common fields
* Add support for zstandard loaders (.zst files). ([#232](https://github.com/turbot/tailpipe-plugin-sdk/issues/232))

## v0.7.2 [2025-06-04]
_Bug fixes_
* `TimeRangeCollectionStateImpl.SetEndTime` now updates end time correctly. ([#207](https://github.com/turbot/tailpipe-plugin-sdk/issues/207))

## v0.7.1 [2025-06-04]
_Bug fixes_

* Fix error handling code which ignores "unknown method SourceCollectionComplete" errors caused by source-plugin version mismatch. ([#222](https://github.com/turbot/tailpipe-plugin-sdk/issues/222))

## v0.7.0 [2025-06-03]
_Whats new_
* End time for collection state should be set to collection end time (or just collection timestamp if no end time set) if source collection is successful. ([#207](https://github.com/turbot/tailpipe-plugin-sdk/issues/207))
  * Add `ToTime` to `CollectRequest` and `SourceParams` - default to collection time
  * add `OnCollectionComplete` to RowSource interface - implement in `RowSourceImpl` to set the collection state end time to the collection 'to' time
  * RowSourceImpl maintains error count incremented from NotifyError, OnCollectionComplete only sets collection state end time if error count is zero
  * Add `RowSourceDecorator` to wrap calls to collect, ensuring that `OnCollected` is called
  * Add SourceCollectionComplete GRPC call - plugin source wrapper calls this from its OnCollectionComplete
* Update ArtifactCollectionStateImpl.GetEndTime to NOT default to LastModifiedTime if there is no end time - instead return zero time. ([#212](https://github.com/turbot/tailpipe-plugin-sdk/issues/212))

## v0.6.1 [2025-05-16]
_Bug fixes_
* Update checkJsonlSize to skip check if no min size is set. ([#204](https://github.com/turbot/tailpipe-plugin-sdk/issues/204))

## v0.6.0 [2025-05-16]
_Whats new_
* Add support for zip artifact loaders. ([#195](https://github.com/turbot/tailpipe-plugin-sdk/issues/195))

## v0.5.1 [2025-04-25]
_Whats new_
* When describing a source, include all properties. ([#199](https://github.com/turbot/tailpipe-plugin-sdk/issues/199))

## v0.5.0 [2025-04-25]
_Whats new_
* Add support for enforcing size limits on temporary directory `max_temp_dir_mb` by limiting total JSONL disk usage. ([#192](https://github.com/turbot/tailpipe-plugin-sdk/issues/192))
  * Set max JSON size to 75% of the configured max_temp_cache_mb
  * Implement Pause and Resume functionality for RowSource conversion, collection to be paused to allow JSON to be processed and removed from disk.
  * Add GetFolderFileSizeMb to support conversion-time file size assessments.

## v0.4.0 [2025-04-25]

_Whats new_
* Add WithHeaderRowNotification RowSourceOption, which can be set to enable a mapper to be notified of the header row of an artifact. ([#186](https://github.com/turbot/tailpipe-plugin-sdk/issues/186))

_Bug fixes_
* Fix source file error for custom tables when using S3 or other external source. ([#188](https://github.com/turbot/tailpipe-plugin-sdk/issues/188))

## v0.3.1 [2025-04-16]

_Bug fixes_
* Fix Column level `null_if` not being respected. ([#182](https://github.com/turbot/tailpipe-plugin-sdk/issues/182))
* Fix missing required column is not being reported as a row error.  ([#181](https://github.com/turbot/tailpipe-plugin-sdk/issues/181))

## v0.3.0 [2025-04-15]
_Whats new_
* Add support for custom tables.
* Add `jsonl` and `delimited` formats, which support for directly converting delimited and JSONL source files to JSONL.
* Add `ConversionSchema` which specifies a separate set of source columns to the output columns
* Improve error handling - allow for non fatal source errors. ([#148](https://github.com/turbot/tailpipe-plugin-sdk/issues/148))
* Update goduckdb to v2.1.0

_Bug fixes_
* Update schema type normalisation to only adjust the case of type names, not struct/array/union member properties.
* Fix race condition in `OnArtifactDownloaded` - add additional wait group increment/decrement.

## v0.2.0 [2025-04-02]
_Whats new_
- Add support for custom tables to use formats from other plugins, and to use format presets. ([#41](https://github.com/turbot/tailpipe-plugin-sdk/issues/41))
- Add support for directly converting delimited and JSONL source files to enriched JSONL intermediate files.
- Add support for applying duck db transforms to column values. 

_Bug fixes_
- Fix race condition in OnArtifactDownloaded.OnArtifactDownloaded - add additional wag group increment.decrement
- Add ColumnSchema.Clone to fix inconsistent behavior when copying column schemas.
- Update TableSchema.MapRow to handle null values

## v0.1.1 [2025-02-10]
_Bug fixes_
- Artifacts in root location now stored correctly in collection state.
- GonxMapper no longer attempts to call schema.MapRow when schema is nil/empty.

## v0.1.0 [2025-01-20]

Initial SDK release.

