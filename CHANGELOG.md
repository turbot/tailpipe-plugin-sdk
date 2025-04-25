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

