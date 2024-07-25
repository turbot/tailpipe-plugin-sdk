package row_source

import "github.com/turbot/tailpipe-plugin-sdk/artifact"

type ArtifactRowSourceOptions func(*ArtifactRowSource)

func WithMapper(mappers ...artifact.Mapper) ArtifactRowSourceOptions {
	return func(a *ArtifactRowSource) {
		a.Mappers = append(a.Mappers, mappers...)
	}
}
func WithLoader(loader artifact.Loader) ArtifactRowSourceOptions {
	return func(a *ArtifactRowSource) {
		a.Loader = loader
	}
}
func WithRowPerLine() ArtifactRowSourceOptions {
	return func(a *ArtifactRowSource) {
		a.RowPerLine = true
	}
}
