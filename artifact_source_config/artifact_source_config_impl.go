package artifact_source_config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/go-kit/helpers"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type ArtifactSourceConfigImpl struct {
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	// grok string defining the file layout and allowing metadata to be extracted
	FileLayout *string `hcl:"file_layout,optional"`

	// grok patterns to add to the grok parser used to parse the layout
	Patterns map[string]string `hcl:"patterns,optional"`
}

func (b *ArtifactSourceConfigImpl) Validate() error {
	// parse filters and put into map keyed by property name
	//filterMap, err := helpers2.BuildFilterMap(b.Filters)
	//if err != nil {
	//	return err
	//}
	//b.FilterMap = filterMap
	//
	//// validate the filters - if filters are set, file layout must be set
	//if len(b.Filters) > 0 {
	//	if b.FileLayout == nil {
	//		return fmt.Errorf("filters are set, but file_layout is not set")
	//	}
	//
	//	// validate all fields referred to in the filters exist in the filter layout
	//	metadataProperties := helpers.SliceToLookup(helpers2.ExtractNamedGroupsFromGrok(*b.FileLayout))
	//	// we have already pulled out the property names in the map keys
	//	for k := range b.FilterMap {
	//		if _, ok := metadataProperties[k]; !ok {
	//			return fmt.Errorf("filter %s refers to a property not in the file layout", k)
	//		}
	//	}
	//}
	//
	return nil
}

func (b *ArtifactSourceConfigImpl) Identifier() string {
	return "artifact_source"
}

func (b *ArtifactSourceConfigImpl) GetFileLayout() *string {
	return b.FileLayout
}

func (b *ArtifactSourceConfigImpl) GetPatterns() map[string]string {
	return b.Patterns
}

func (b *ArtifactSourceConfigImpl) DefaultTo(other ArtifactSourceConfig) {
	if helpers.IsNil(other) {
		return
	}

	if other.GetFileLayout() != nil && b.FileLayout == nil {
		b.FileLayout = other.GetFileLayout()
	}
}

// AsProto converts ArtifactSourceConfigImpl to its Protobuf representation.
// used to pass default config to an external-plugin source
func (b *ArtifactSourceConfigImpl) AsProto() *proto.ArtifactSourceConfig {
	return &proto.ArtifactSourceConfig{
		FileLayout: typehelpers.SafeString(b.FileLayout),
		Patterns:   b.Patterns,
	}
}

func ArtifactSourceConfigBaseFromProto(pb *proto.ArtifactSourceConfig) *ArtifactSourceConfigImpl {
	return &ArtifactSourceConfigImpl{
		FileLayout: utils.ToPointer(pb.FileLayout),
		Patterns:   pb.Patterns,
	}
}
