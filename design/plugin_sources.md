# Plugin Sources

## Issues
- how do we pass options across GRPC
The options are implemented by interface functions

ArtifactSource
SetRowPerLine(b bool)
SetSkipHeaderRow(b bool)
SetDefaultConfig(config *artifact_source_config.ArtifactSourceConfigBase)
SetCollectionStateJSON

// these are implemented by the artifact source NOT the row source

SetLoader(loader artifact_loader.Loader)
SetExtractor(extractor Extractor)

implement separate interfaces for these rather that the full artifact 
source interface so we can easily stub them

implement them with version which sets a config which can be converted to proto
we can then pass the config across GRPC and convert back to config and check which properties are set, 
then set these properties on 