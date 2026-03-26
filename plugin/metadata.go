package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/pipe-fittings/v2/ociinstaller"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/types"
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
	metadata := types.DescribeResponseFromProto(describeResponse).AsMetadataMap()

	// OciInstaller
	app_specific.DefaultImageRepoActualURL = "ghcr.io/turbot/tailpipe"
	app_specific.DefaultImageRepoDisplayURL = "hub.tailpipe.io"
	imageRef := ociinstaller.NewImageRef(p.Identifier()).DisplayImageRef()

	// build an installed version object
	v := &versionfile.InstalledVersion{
		Name:          imageRef,
		StructVersion: versionfile.InstalledVersionStructVersion,
		Metadata:      metadata,
		// The PrintMetadata mechanism is in place to allow locally built plugins to report their version to we just put 'local' here
		Version: "local",
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
