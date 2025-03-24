package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type JsonLines struct {
	Name        string `hcl:",label"`
	Description string `hcl:"description,optional"`
}

func NewJsonLines() Format {
	return &JsonLines{}
}

func (d *JsonLines) Validate() error {
	return nil
}

// GetName returns the name of this format instance
func (d *JsonLines) GetName() string {
	return d.Name
}

// SetName sets the name of this format instance
func (d *JsonLines) SetName(name string) {
	d.Name = name
}

// GetDescription returns the description of this format instance
func (d *JsonLines) GetDescription() string {
	return d.Description
}

// GetProperties returns the format as a string which can be included in the introspection response

func (d *JsonLines) GetProperties() map[string]string {
	properties := make(map[string]string)

	return properties
}

// Identifier returns the format type identifier
func (d *JsonLines) Identifier() string {
	return constants.SourceFormatJsonLines
}

func (d *JsonLines) GetRegex() (string, error) {
	// the JsonL format does not support regex
	return "N/A", nil
}

func (d *JsonLines) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	// the JsonL format does not support mapper
	return nil, fmt.Errorf("JsonLines format does not support a mapper")
}
