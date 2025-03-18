package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"os"
)

func PrintMetadata(pluginFunc PluginFunc) int {
	// create the plugin
	p, err := pluginFunc()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to create plugin: %s\n", err) //nolint:forbidigo // expected ui output
		os.Exit(1)
	}
	// describe the plugin
	describeResponse, err := p.Describe(context.Background(), &proto.DescribeRequest{})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to describe plugin: %s\n", err) //nolint:forbidigo // expected ui output
		return 1
	}

	// convert the describe response to a metadata map
	metadata := DescribeResponseFromProto(describeResponse).AsMetadataMap()

	// build an installed version object
	v := &versionfile.InstalledVersion{
		Name:          p.Identifier(),
		StructVersion: versionfile.InstalledVersionStructVersion,
		Metadata:      metadata,
	}
	// serialize to JSON
	installedVersionJson, err := json.Marshal(v)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to marshal metadata: %s\n", err) //nolint:forbidigo // expected ui output
		return 1
	}

	// write to stdout
	fmt.Println(string(installedVersionJson)) //nolint:forbidigo // expected ui output
	return 0
}
