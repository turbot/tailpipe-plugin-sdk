package formats

import "github.com/turbot/tailpipe-plugin-sdk/grpc/proto"

type FormatDescriptionMap map[string]*FormatDescription

func (f FormatDescriptionMap) ToProto() map[string]*proto.FormatDescription {
	pb := map[string]*proto.FormatDescription{}

	for name, formatDescription := range f {
		pb[name] = formatDescription.ToProto()
	}
	return pb
}

func FormatMapFromProto(pb map[string]*proto.FormatDescription) FormatDescriptionMap {
	res := FormatDescriptionMap{}
	for name, formatDescription := range pb {
		res[name] = FormatDescriptionFromProto(formatDescription)
	}
	return res
}

// FormatDescription is a struct which contains introspection data about a format
// - it is used in the Describe call to pas format information
type FormatDescription struct {
	Type        string
	Name        string
	Description string
	Regex       string
	Properties  map[string]string
}

func FormatDescriptionFromProto(pb *proto.FormatDescription) *FormatDescription {
	return &FormatDescription{
		Type:        pb.Type,
		Name:        pb.Name,
		Description: pb.Description,
		Properties:  pb.Properties,
		Regex:       pb.Regex,
	}
}

func (f *FormatDescription) ToProto() *proto.FormatDescription {
	return &proto.FormatDescription{
		Type:        f.Type,
		Name:        f.Name,
		Description: f.Description,
		Properties:  f.Properties,
		Regex:       f.Regex,
	}
}

func (f *FormatDescription) FullName() string {
	return f.Type + "." + f.Name
}
