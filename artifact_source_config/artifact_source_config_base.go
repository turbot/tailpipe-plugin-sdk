package artifact_source_config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/turbot/go-kit/helpers"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/filter"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type ArtifactSourceConfigBase struct {
	// required to allow partial decoding
	Remain hcl.Body `hcl:",remain" json:"-"`

	// grok string defining the file layout and allowing metadata to be extracted
	FileLayout *string `hcl:"file_layout,optional"`

	// grok patterns to add to the grok parser used to parse the layout
	Patterns map[string]string `hcl:"patterns,optional"`

	// TODO SORT THIS NEEDED OR NOT>??>?
	// list of filters to apply to the path segments
	// note: each filter must refer to a single property only
	// filters are ANDED together
	//Filters []string `hcl:"filters,optional"`

	// map of parsed filters, keyed by target property
	FilterMap map[string]*filter.SqlFilter
}

func (b *ArtifactSourceConfigBase) Validate() error {
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

func (b *ArtifactSourceConfigBase) Identifier() string {
	return "artifact_source"
}

func (b *ArtifactSourceConfigBase) GetFileLayout() *string {
	return b.FileLayout
}

func (b *ArtifactSourceConfigBase) GetPatterns() map[string]string {
	return b.Patterns
}

func (b *ArtifactSourceConfigBase) DefaultTo(other ArtifactSourceConfig) {
	if helpers.IsNil(other) {
		return
	}

	if other.GetFileLayout() != nil && b.FileLayout == nil {
		b.FileLayout = other.GetFileLayout()
	}
}

// AsProto converts ArtifactSourceConfigBase to its Protobuf representation.
// used to pass default config to an external-plugin source
func (b *ArtifactSourceConfigBase) AsProto() *proto.ArtifactSourceConfig {
	return &proto.ArtifactSourceConfig{
		FileLayout: typehelpers.SafeString(b.FileLayout),
		Patterns:   b.Patterns,
	}
}
