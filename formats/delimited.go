package formats

import (
	"fmt"
	"github.com/turbot/pipe-fittings/v2/utils"
	"strings"

	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// DefaultDelimited is the default Delimited format - this is exported by the core plugin
var DefaultDelimited = &Delimited{
	Name:        "default",
	Description: "Default Delimited format",
	Delimiter:   utils.ToPointer(","),
	Header:      utils.ToPointer(true),
}

// TODO REVIEW OPTIONS AND LOOK FOR COMMONALITY
type Delimited struct {
	Name        string `hcl:",label"`
	Description string `hcl:"description,optional"`
	// Option to skip type detection for CSV parsing and assume all columns to be of type VARCHAR
	AllVarchar *bool `hcl:"all_varchar,optional"`

	// Option to allow the conversion of quoted values to NULL values
	AllowQuotedNulls *bool `hcl:"allow_quoted_nulls,optional"`

	// Specifies the date format to use when parsing dates.
	//DateFormat *string

	// The decimal separator of numbers.
	DecimalSeparator *string `hcl:"decimal_separator,optional"`

	// Specifies the delimiter character that separates columns within each row (line) of the file.
	Delimiter *string `hcl:"delimiter,optional"`

	// Specifies the string that should appear before a data character sequence that matches the quote value.
	Escape *string `hcl:"escape,optional"`

	// Whether or not an extra filename column should be included in the result.
	Filename *bool `hcl:"filename,optional"`

	// Do not match the specified columns' values against the NULL string.
	// In the default case where the NULL string is empty,
	// this means that empty values will be read as zero-length strings rather than NULLs.
	ForceNotNull *[]string `hcl:"force_not_null,optional"`

	// Specifies that the file contains a header line with the names of each column in the file.
	Header *bool `hcl:"header,optional"`

	// Option to ignore any parsing errors encountered – and instead ignore rows with errors.
	IgnoreErrors *bool `hcl:"ignore_errors,optional"`

	// The maximum line size in bytes.
	MaxLineSize *int `hcl:"max_line_size,optional"`

	// Set the new line character(s) in the file. Options are '\r','\n', or '\r\n'.
	// Note that the CSV parser only distinguishes between single-character and double-character line delimiters.
	// Therefore, it does not differentiate between '\r' and '\n'.
	NewLine *string `hcl:"new_line,optional"`

	// Boolean value that specifies whether or not column names should be normalized,
	// removing any non-alphanumeric characters from them.
	NormalizeNames *bool `hcl:"normalize_names,optional"`

	// If this option is enabled, when a row lacks columns, it will pad the remaining columns on the right with NULL values.
	NullPadding *bool `hcl:"null_padding,optional"`

	// Specifies the string that represents a NULL value or (since v0.10.2) a list of strings that represent a NULL value.
	NullStr *string `hcl:"null_str,optional"`

	// Specifies the quoting string to be used when a data value is quoted.
	Quote *string `hcl:"quote,optional"`

	// The number of sample rows for auto detection of parameters.
	SampleSize *int `hcl:"sample_size,optional"`

	// Specifies the date format to use when parsing timestamps
	TimestampFormat *string `hcl:"timestamp_format,optional"`
}

func NewDelimited() Format {
	return &Delimited{}
}

func (d *Delimited) Validate() error {
	return nil
}

// GetName returns the name of this format instance
func (d *Delimited) GetName() string {
	return d.Name
}

// SetName sets the name of this format instance
func (d *Delimited) SetName(name string) {
	d.Name = name
}

// GetDescription returns the description of this format instance
func (d *Delimited) GetDescription() string {
	return d.Description
}

// GetProperties returns the format as a string which can be included in the introspection response

func (d *Delimited) GetProperties() map[string]string {
	properties := make(map[string]string)

	if d.AllVarchar != nil {
		properties["all_varchar"] = fmt.Sprintf("%v", *d.AllVarchar)
	}
	if d.AllowQuotedNulls != nil {
		properties["allow_quoted_nulls"] = fmt.Sprintf("%v", *d.AllowQuotedNulls)
	}
	if d.DecimalSeparator != nil {
		properties["decimal_separator"] = *d.DecimalSeparator
	}
	if d.Delimiter != nil {
		properties["delimiter"] = *d.Delimiter
	}
	if d.Escape != nil {
		properties["escape"] = *d.Escape
	}
	if d.Filename != nil {
		properties["filename"] = fmt.Sprintf("%v", *d.Filename)
	}
	if d.ForceNotNull != nil && len(*d.ForceNotNull) > 0 {
		properties["force_not_null"] = strings.Join(*d.ForceNotNull, ",")
	}
	if d.Header != nil {
		properties["header"] = fmt.Sprintf("%v", *d.Header)
	}
	if d.IgnoreErrors != nil {
		properties["ignore_errors"] = fmt.Sprintf("%v", *d.IgnoreErrors)
	}
	if d.MaxLineSize != nil {
		properties["max_line_size"] = fmt.Sprintf("%d", *d.MaxLineSize)
	}
	if d.NewLine != nil {
		properties["new_line"] = *d.NewLine
	}
	if d.NormalizeNames != nil {
		properties["normalize_names"] = fmt.Sprintf("%v", *d.NormalizeNames)
	}
	if d.NullPadding != nil {
		properties["null_padding"] = fmt.Sprintf("%v", *d.NullPadding)
	}
	if d.NullStr != nil {
		properties["null_str"] = *d.NullStr
	}
	if d.Quote != nil {
		properties["quote"] = *d.Quote
	}
	if d.SampleSize != nil {
		properties["sample_size"] = fmt.Sprintf("%d", *d.SampleSize)
	}
	if d.TimestampFormat != nil {
		properties["timestamp_format"] = *d.TimestampFormat
	}

	return properties
}

// Identifier returns the format type identifier
func (d *Delimited) Identifier() string {
	return constants.SourceFormatDelimited
}

func (d *Delimited) GetRegex() (string, error) {
	// the delimited format does not support regex
	return "N/A", nil
}

func (d *Delimited) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	panic("implement me")
}

// GetCsvOpts converts the Delimited configuration into a slice of CSV options strings
// in the format expected by DuckDb read_csv function
func (d *Delimited) GetCsvOpts() []string {
	var opts []string

	// Handle delimiter - default to comma if not specified
	delimiter := ","
	if d.Delimiter != nil {
		delimiter = *d.Delimiter
	}
	opts = append(opts, fmt.Sprintf("delim '%s'", delimiter))

	// Handle header - default to true if not specified
	header := true
	if d.Header != nil {
		header = *d.Header
	}

	opts = append(opts, fmt.Sprintf("header %v", strings.ToLower(fmt.Sprintf("%v", header))))

	if d.AllVarchar != nil {
		opts = append(opts, fmt.Sprintf("all_varchar=%v", *d.AllVarchar))
	}

	if d.AllowQuotedNulls != nil {
		opts = append(opts, fmt.Sprintf("allow_quoted_nulls=%v", *d.AllowQuotedNulls))
	}

	if d.DecimalSeparator != nil {
		opts = append(opts, fmt.Sprintf("decimal_separator='%s'", *d.DecimalSeparator))
	}

	if d.Escape != nil {
		opts = append(opts, fmt.Sprintf("escape='%s'", *d.Escape))
	}

	if d.Filename != nil {
		opts = append(opts, fmt.Sprintf("filename=%v", *d.Filename))
	}

	if d.ForceNotNull != nil && len(*d.ForceNotNull) > 0 {
		forceNotNullValues := strings.Join(*d.ForceNotNull, ",")
		opts = append(opts, fmt.Sprintf("force_not_null=%s", forceNotNullValues))
	}

	if d.IgnoreErrors != nil {
		opts = append(opts, fmt.Sprintf("ignore_errors=%v", *d.IgnoreErrors))
	}

	if d.MaxLineSize != nil {
		opts = append(opts, fmt.Sprintf("max_line_size=%d", *d.MaxLineSize))
	}

	if d.NewLine != nil {
		opts = append(opts, fmt.Sprintf("new_line='%s'", *d.NewLine))
	}

	if d.NormalizeNames != nil {
		opts = append(opts, fmt.Sprintf("normalize_names=%v", *d.NormalizeNames))
	}

	if d.NullPadding != nil {
		opts = append(opts, fmt.Sprintf("null_padding=%v", *d.NullPadding))
	}

	if d.NullStr != nil {
		opts = append(opts, fmt.Sprintf("null_str='%s'", *d.NullStr))
	}

	if d.Quote != nil {
		opts = append(opts, fmt.Sprintf("quote='%s'", *d.Quote))
	}

	if d.SampleSize != nil {
		opts = append(opts, fmt.Sprintf("sample_size=%d", *d.SampleSize))
	}

	if d.TimestampFormat != nil {
		opts = append(opts, fmt.Sprintf("timestamp_format='%s'", *d.TimestampFormat))
	}

	return opts
}
