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

