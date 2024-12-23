package schema

// SourceEnrichment - is a set of metadata about a row - this is built by the row source and passed
// to the enrichment
type SourceEnrichment struct {
	// any tp_field which the source can populated
	CommonFields CommonFields
	// a map of metadata values the source has extracted - perhaps by parsing th artifact path with a grok pattern
	Metadata map[string][]byte
}
