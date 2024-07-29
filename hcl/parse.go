package hcl

import (
	"bytes"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
	"log/slog"
)

// ValueType is a constraint that permits only value types (non-pointer types)
type ValueType interface {
	comparable
}

func ParseConfig[T Config](configData *Data) (T, []byte, error) {
	// declare empty struct instance
	var target T

	// parse the config
	file, diags := hclsyntax.ParseConfig(configData.ConfigData, configData.Filename, configData.Pos)
	if diags.HasErrors() {
		slog.Error("ParseConfig: Failed to parse config into hcl file", "diags", diags)
		return target, nil, error_helpers.HclDiagsToError("failed to parse config", diags)
	}
	// create empty eval context
	evalCtx := &hcl.EvalContext{
		Variables: make(map[string]cty.Value),
		Functions: make(map[string]function.Function),
	}
	// decode the body into the target struct - this also builds a buffer containing all unknown HCL properties
	decodeDiags := gohcl.DecodeBody(file.Body, evalCtx, &target)

	// handle unknown HCL properties
	unknownConfig, decodeDiags := handleDecodeDiags(file, decodeDiags)

	diags = append(diags, decodeDiags...)
	if diags.HasErrors() {
		slog.Error("ParseConfig: Failed to decode config body", "diags", diags)
		return target, nil, error_helpers.HclDiagsToError("failed to parse config", diags)
	}
	// return the struct by value
	return target, unknownConfig, nil
}

func handleDecodeDiags(file *hcl.File, decodeDiags hcl.Diagnostics) ([]byte, hcl.Diagnostics) {
	var diags hcl.Diagnostics

	var unknownHclList [][]byte
	// for each  "Unsupported argument" diag, extract the unknown hcl and remove the diag
	for _, diag := range decodeDiags {
		if diag.Severity == hcl.DiagError && diag.Summary == "Unsupported argument" {
			// extract the unknown hcl
			u := extractUnknownHcl(file, diag.Subject)
			// if we succeded in extracting the unknown hcl, add it to the list
			if u != nil {
				unknownHclList = append(unknownHclList, u)
				continue
			}
			// otherwise fall through to add the error
		}
		diags = append(diags, diag)
	}

	unknown := bytes.Join(unknownHclList, []byte("\n"))
	return unknown, diags

}

func extractUnknownHcl(file *hcl.File, subject *hcl.Range) []byte {
	// get the start and end positions of the unknown hcl
	start := subject.Start
	end := subject.End

	// extract the unknown hcl
	// get the property name
	property := string(file.Bytes[start.Byte:end.Byte])
	// now look for this attribute in the file
	attr, ok := (file.Body).(*hclsyntax.Body).Attributes[property]
	if !ok {
		return nil
	}
	// now get ther bytes for the attrribute
	unknownHcl := file.Bytes[attr.Range().Start.Byte:attr.Range().End.Byte]

	return unknownHcl
}
