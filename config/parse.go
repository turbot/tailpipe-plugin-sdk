package config

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
)

// ValueType is a constraint that permits only value types (non-pointer types)
type ValueType interface {
	comparable
}

func ParseConfig[T any](configString []byte, filename string, startPos hcl.Pos, target *T) error {
	// parse the config
	file, diags := hclsyntax.ParseConfig(configString, filename, startPos)
	if diags.HasErrors() {
		return error_helpers.HclDiagsToError("failed to parse config", diags)
	}
	// create empty eval context
	evalCtx := &hcl.EvalContext{
		Variables: make(map[string]cty.Value),
		Functions: make(map[string]function.Function),
	}
	// decode the body into the target struct
	moreDiags := gohcl.DecodeBody(file.Body, evalCtx, target)
	diags = append(diags, moreDiags...)
	if diags.HasErrors() {
		return error_helpers.HclDiagsToError("failed to parse config", diags)
	}
	// return the struct by value
	return nil
}
