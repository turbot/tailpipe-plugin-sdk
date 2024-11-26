package row_source

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

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

type SourceMetadata struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

func (s *SourceMetadata) ToProto() *proto.SourceMetadata {
	return &proto.SourceMetadata{
		Name:        s.Name,
		Description: s.Description,
	}
}

func SourceMetadataFromProto(v *proto.SourceMetadata) *SourceMetadata {
	return &SourceMetadata{
		Name:        v.Name,
		Description: v.Description,
	}
}
