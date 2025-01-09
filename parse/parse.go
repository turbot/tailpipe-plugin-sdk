package parse

import (
	"fmt"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/error_helpers"
	pf_parse "github.com/turbot/pipe-fittings/parse"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
	"log/slog"
)

// ParseConfig parses the HCL config and returns the struct
// Config is an interface that all configuration structs must implement
func ParseConfig[T Config](configData types.ConfigData) (T, error) {
	// Create a new instance of the target struct
	target := utils.InstanceOf[T]()
	// verify this config is of correct type
	// this ensures that the ConfigData type (the Identifier) matches the target type (the Identifier of the target)
	id := target.Identifier()
	if id != configData.Identifier() {
		return target, fmt.Errorf("invalid %s type '%s': expected '%s'", configData.GetConfigType(), configData.Identifier(), id)
	}

	// Parse the config
	declRange := configData.GetRange()
	hclBytes := configData.GetHcl()
	file, diags := hclsyntax.ParseConfig(hclBytes, declRange.Filename, declRange.Start)
	if diags != nil && diags.HasErrors() {
		slog.Warn("failed to parse config", "config type", configData.GetConfigType(), "hcl", hclBytes)
		return target, fmt.Errorf("failed to parse %s config: %s", configData.GetConfigType(), diags)
	}

	// Create empty eval context
	evalCtx := &hcl.EvalContext{
		Variables: make(map[string]cty.Value),
		Functions: make(map[string]function.Function),
	}

	decodeDiags := decodeHclBodyWithNestedStructs(file.Body, evalCtx, target)
	// Decode the body into the target struct
	//decodeDiags := gohcl.DecodeBody(file.Body, evalCtx, target)
	diags = append(diags, decodeDiags...)
	if diags.HasErrors() {
		return target, error_helpers.HclDiagsToError(fmt.Sprintf("Failed to decode %s config", configData.GetConfigType()), diags)
	}

	// Return the struct by value
	return target, nil
}

// decodeHclBodyWithNestedStructs decodes the hcl body into the target resource, also decoding into any nested structs
func decodeHclBodyWithNestedStructs(body hcl.Body, evalCtx *hcl.EvalContext, resource any) (diags hcl.Diagnostics) {
	defer func() {
		if r := recover(); r != nil {
			diags = append(diags, &hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  "unexpected error in decodeHclBodyWithNestedStructs",
				Detail:   helpers.ToError(r).Error()})
		}
	}()

	nestedStructs, moreDiags := pf_parse.GetNestedStructValsRecursive(resource)
	diags = append(diags, moreDiags...)

	moreDiags = gohcl.DecodeBody(body, evalCtx, resource)
	diags = append(diags, moreDiags...)

	for _, nestedStruct := range nestedStructs {
		moreDiags := gohcl.DecodeBody(body, evalCtx, nestedStruct)
		diags = append(diags, moreDiags...)
	}

	return diags
}
