package formats

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// DefaultJsonLines is the default JSONL format - this is exported by the core plugin
var DefaultJsonLines = &JsonLines{
	Name:        "default",
	Description: "Default JSONL format",
}

type JsonLines struct {
	Name        string `hcl:",label"`
	Description string `hcl:"description,optional"`
	// Option to define number of sample objects for automatic JSON type detection.
	// Set to -1 to scan the entire input file (if not provided, DuckDB defaults to 20480)
	SampleSize *int `hcl:"sample_size,optional"`
	// Specifies the date format to use when parsing timestamps.
	// (If not provided, DuckDB defaults to the ISO 8601 format)
	DateFormat *string `hcl:"date_format,optional"`
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
	if d.SampleSize != nil {
		properties["sample_size"] = fmt.Sprintf("%d", *d.SampleSize)
	}
	if d.DateFormat != nil {
		properties["date_format"] = *d.DateFormat
	}

	return properties
}

// Identifier returns the format type identifier
func (d *JsonLines) Identifier() string {
	return constants.SourceFormatJsonl
}

func (d *JsonLines) GetRegex() (string, error) {
	// the JsonL format does not support regex
	return "N/A", nil
}

func (d *JsonLines) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	// the JsonL format does not support mapper
	return nil, fmt.Errorf("JsonLines format does not support a mapper")
}

// GetReadJsonOpts converts the Delimited configuration into a slice of CSV options strings
// in the format expected by DuckDb read_csv function
func (d *JsonLines) GetReadJsonOpts() []string {
	var opts []string

	if d.SampleSize != nil {
		opts = append(opts, fmt.Sprintf("sample_size=%d", *d.SampleSize))
	}
	if d.DateFormat != nil {
		opts = append(opts, fmt.Sprintf("date_format='%s'", *d.DateFormat))
	}
	return opts
}
