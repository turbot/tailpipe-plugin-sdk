package types

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

// SourceMetadata is used to describe a source
type SourceMetadata struct {
	Name        string                       `json:"name"`
	Description string                       `json:"description,omitempty"`
	Properties  map[string]*PropertyMetadata `json:"properties,omitempty"`
}

type PropertyMetadata struct {
	Description string `json:"description,omitempty"`
	Type        string `json:"type,omitempty"`
	Required    bool   `json:"required,omitempty"`
	Default     string `json:"default,omitempty"`
}

func (p *PropertyMetadata) ToProto() *proto.PropertyMetadata {
	return &proto.PropertyMetadata{
		Description: p.Description,
		Type:        p.Type,
		Required:    p.Required,
		Default:     p.Default,
	}
}

func PropertyMetadataFromProto(v *proto.PropertyMetadata) *PropertyMetadata {
	return &PropertyMetadata{
		Description: v.Description,
		Type:        v.Type,
		Required:    v.Required,
		Default:     v.Default,
	}
}

func (s *SourceMetadata) ToProto() *proto.SourceMetadata {
	properties := make(map[string]*proto.PropertyMetadata)
	for k, v := range s.Properties {
		properties[k] = v.ToProto()
	}
	return &proto.SourceMetadata{
		Name:        s.Name,
		Description: s.Description,
		Properties:  properties,
	}
}

func SourceMetadataFromProto(v *proto.SourceMetadata) *SourceMetadata {
	properties := make(map[string]*PropertyMetadata)
	for k, v := range v.Properties {
		properties[k] = PropertyMetadataFromProto(v)
	}
	return &SourceMetadata{
		Name:        v.Name,
		Description: v.Description,
		Properties:  properties,
	}
}

// SourceMetadataMap is a map of source metadata - this is returned from the DescribeSources call
type SourceMetadataMap map[string]*SourceMetadata

func (s SourceMetadataMap) ToProto() map[string]*proto.SourceMetadata {
	var res = make(map[string]*proto.SourceMetadata, len(s))

	for k, v := range s {
		res[k] = v.ToProto()
	}
	return res
}

func SourceMetadataMapFromProto(p map[string]*proto.SourceMetadata) SourceMetadataMap {
	var res = make(SourceMetadataMap, len(p))

	for k, v := range p {
		res[k] = SourceMetadataFromProto(v)
	}
	return res
}
