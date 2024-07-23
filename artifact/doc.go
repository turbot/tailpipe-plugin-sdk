// Package artifact provides types and functions for loading and processing rows using an
// ArtifactRowSource.
//
// Artifacts are defined as some entity which contains a collection of rows, which must be extracted/processed in
// some way to produce 'rsaw'rows which can be streamed to a collection.
//
// The ArtifactRowSource is a type which can be used to extract rows from an artifact, and process them using a
// This package includes the necessary components to handle generating a stream of 'raw' rows from artifacts:
// - sources:
// - loaders:
// - mappers:
package artifact
