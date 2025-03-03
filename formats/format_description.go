package formats

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type FormatMap map[string][]*FormatDescription

func (f FormatMap) ToProto() *proto.FormatMap {
	pb := &proto.FormatMap{
		Formats: make(map[string]*proto.FormatDescriptions),
	}
	for ty, formats := range f {
		formatDescriptions := &proto.FormatDescriptions{}
		for _, format := range formats {
			formatDescriptions.Descriptions = append(formatDescriptions.Descriptions, format.AsProto())
		}
		pb.Formats[ty] = formatDescriptions
	}
	return pb
}

func FormatMapFromProto(pb []*proto.FormatDescription) FormatMap {
	fm := make(FormatMap)
	for _, formatDescription := range pb {
		fm[formatDescription.Type] = append(fm[formatDescription.Type], FormatDescriptionFromProto(formatDescription))
	}
	return fm
}

type FormatDescription struct {
	Type        string
	Name        string
	Description string
}

func FormatDescriptionFromProto(pb *proto.FormatDescription) *FormatDescription {
	return &FormatDescription{
		Type:        pb.Type,
		Name:        pb.Name,
		Description: pb.Description,
	}
}

func (f *FormatDescription) AsProto() *proto.FormatDescription {
	return &proto.FormatDescription{
		Type:        f.Type,
		Name:        f.Name,
		Description: f.Description,
	}
}
