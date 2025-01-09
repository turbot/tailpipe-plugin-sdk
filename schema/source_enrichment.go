package schema

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

// SourceEnrichment - is a set of metadata about a row - this is built by the row source and passed
// to the enrichment
type SourceEnrichment struct {
	// a map of metadata values the source has extracted - perhaps by parsing th artifact path with a grok pattern
	Metadata map[string]string
	// CommonFields - a set of common fields that are added to every row
	CommonFields CommonFields
}

func NewSourceEnrichment(metadata map[string]string) *SourceEnrichment {
	res := &SourceEnrichment{
		Metadata: metadata,
	}
	// initialise common fields from metadata
	res.CommonFields.InitialiseFromMap(metadata)
	return res
}

func (s *SourceEnrichment) ToProto() *proto.SourceEnrichment {
	// convert
	return &proto.SourceEnrichment{
		CommonFields: s.CommonFields.AsMap(),
		Metadata:     s.Metadata,
	}
}

func SourceEnrichmentFromProto(p *proto.SourceEnrichment) *SourceEnrichment {
	res := &SourceEnrichment{
		Metadata: p.Metadata,
	}
	res.CommonFields.InitialiseFromMap(p.CommonFields)
	return res
}
