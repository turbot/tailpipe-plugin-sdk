package hcl

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
)

// ParseConfig parses the HCL config and returns the struct
// Config is an interface that all configuration structs must implement
func ParseConfig[T Config](configData *Data, target T) (T, error) {
	// Parse the config
	file, diags := hclsyntax.ParseConfig(configData.ConfigData, configData.Filename, configData.Pos)
	if diags.HasErrors() {
		return target, fmt.Errorf("failed to parse config: %s", diags)
	}

	// Create empty eval context
	evalCtx := &hcl.EvalContext{
		Variables: make(map[string]cty.Value),
		Functions: make(map[string]function.Function),
	}

	// Decode the body into the target struct
	decodeDiags := gohcl.DecodeBody(file.Body, evalCtx, target)

	diags = append(diags, decodeDiags...)
	if diags.HasErrors() {
		return target, error_helpers.HclDiagsToError("Failed to decode config", diags)
	}

	// Return the struct by value
	return target, nil
}
