package hcl

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
)

// ValueType is a constraint that permits only value types (non-pointer types)
type ValueType interface {
	comparable
}

// ParseConfig parses the HCL config and returns the struct
// Config is an interface that all configuration structs must implement
// it usually has pointer receivers, which means T is actually a pointer to the struct
// so we have to do some work to convert to a value type for the parsing, and then back the original type
func ParseConfig[T Config](configData *Data, target T) (T, []byte, error) {
	// Parse the config
	file, diags := hclsyntax.ParseConfig(configData.ConfigData, configData.Filename, configData.Pos)
	if diags.HasErrors() {
		return target, nil, fmt.Errorf("failed to parse config: %s", diags)
	}

	// Create empty eval context
	evalCtx := &hcl.EvalContext{
		Variables: make(map[string]cty.Value),
		Functions: make(map[string]function.Function),
	}

	// Decode the body into the target struct
	decodeDiags := gohcl.DecodeBody(file.Body, evalCtx, target)

	// Handle unknown HCL properties
	unknownConfig, decodeDiags := handleDecodeDiags(file, decodeDiags)

	diags = append(diags, decodeDiags...)
	if diags.HasErrors() {
		return target, nil, fmt.Errorf("failed to decode config: %s", diags)
	}

	// Return the struct by value
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
