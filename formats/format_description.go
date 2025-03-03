package formats

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type FormatDescriptionMap map[string][]*FormatDescription

func (f FormatDescriptionMap) ToProto() *proto.FormatMap {
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

func FormatMapFromProto(pb *proto.FormatMap) FormatDescriptionMap {
	fm := make(FormatDescriptionMap)
	for ty, formatDescriptions := range pb.Formats {
		var formats []*FormatDescription
		for _, formatDescription := range formatDescriptions.Descriptions {
			formats = append(formats, FormatDescriptionFromProto(formatDescription))
		}
		fm[ty] = formats
	}
	return fm
}

// FormatDescription is a struct which contains introspection data about a format
// - it is used in the Describe call to pas format information
type FormatDescription struct {
	Type         string
	Name         string
	Description  string
	Regex  		 string
	Properties map[string]string
}

func FormatDescriptionFromProto(pb *proto.FormatDescription) *FormatDescription {
	return &FormatDescription{
		Type:        pb.Type,
		Name:        pb.Name,
		Description: pb.Description,
		Properties: pb.Properties,
		Regex: pb.Regex,
	}
}

func (f *FormatDescription) AsProto() *proto.FormatDescription {
	return &proto.FormatDescription{
		Type:        f.Type,
		Name:        f.Name,
		Description: f.Description,
		Properties: f.Properties,
		Regex: f.Regex,
	}
}
