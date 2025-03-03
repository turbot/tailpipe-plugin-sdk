package formats

import (
	"fmt"
	"strings"

	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

type Delimited struct {
	Name        string `hcl:",label"`
	Description string `hcl:"description,optional"`
	// Option to skip type detection for CSV parsing and assume all columns to be of type VARCHAR
	AllVarchar *bool

	// Option to allow the conversion of quoted values to NULL values
	AllowQuotedNulls *bool

	// Specifies the date format to use when parsing dates.
	//DateFormat *string

	// The decimal separator of numbers.
	DecimalSeparator *string

	// Specifies the delimiter character that separates columns within each row (line) of the file.
	Delimiter *string

	// Specifies the string that should appear before a data character sequence that matches the quote value.
	Escape *string

	// Whether or not an extra filename column should be included in the result.
	Filename *bool

	// Do not match the specified columns' values against the NULL string.
	// In the default case where the NULL string is empty,
	// this means that empty values will be read as zero-length strings rather than NULLs.
	ForceNotNull *[]string

	// Specifies that the file contains a header line with the names of each column in the file.
	Header *bool

	// Option to ignore any parsing errors encountered – and instead ignore rows with errors.
	IgnoreErrors *bool

	// The maximum line size in bytes.
	MaxLineSize *int

	// Set the new line character(s) in the file. Options are '\r','\n', or '\r\n'.
	// Note that the CSV parser only distinguishes between single-character and double-character line delimiters.
	// Therefore, it does not differentiate between '\r' and '\n'.
	NewLine *string

	// Boolean value that specifies whether or not column names should be normalized,
	// removing any non-alphanumeric characters from them.
	NormalizeNames *bool

	// If this option is enabled, when a row lacks columns, it will pad the remaining columns on the right with NULL values.
	NullPadding *bool

	// Specifies the string that represents a NULL value or (since v0.10.2) a list of strings that represent a NULL value.
	NullStr *string

	// Specifies the quoting string to be used when a data value is quoted.
	Quote *string

	// The number of sample rows for auto detection of parameters.
	SampleSize *int

	// Specifies the date format to use when parsing timestamps
	TimestampFormat *string
}

func NewDelimited() Format {
	return &Delimited{}
}

func (c *Delimited) Validate() error {
	return nil
}

// GetName returns the name of this format instance
func (c *Delimited) GetName() string {
	return c.Name
}

// GetDescription returns the description of this format instance
func (c *Delimited) GetDescription() string {
	return c.Description
}

// GetFormatString returns the format as a string which can be included in the introspection response

func (c *Delimited) GetFormatString() string {
	panic ("implement me - build string containing all the options")
}

// Identifier returns the format type identifier
func (c *Delimited) Identifier() string {
	return constants.SourceFormatDelimited
}

func (c *Delimited) GetRegex() (string, error) {
	// the delimited format does not support regex
	return "N/A", nil
}

func (c *Delimited) GetMapper() (mappers.Mapper[*types.DynamicRow], error) {
	panic("implement me")
}

// GetCsvOpts converts the Delimited configuration into a slice of CSV options strings
// in the format expected by DuckDb read_csv function
func (c *Delimited) GetCsvOpts() []string {
	var opts []string

	if c.AllVarchar != nil {
		opts = append(opts, fmt.Sprintf("all_varchar=%v", *c.AllVarchar))
	}

	if c.AllowQuotedNulls != nil {
		opts = append(opts, fmt.Sprintf("allow_quoted_nulls=%v", *c.AllowQuotedNulls))
	}

	if c.DecimalSeparator != nil {
		opts = append(opts, fmt.Sprintf("decimal_separator='%s'", *c.DecimalSeparator))
	}

	if c.Delimiter != nil {
		opts = append(opts, fmt.Sprintf("delimiter='%s'", *c.Delimiter))
	}

	if c.Escape != nil {
		opts = append(opts, fmt.Sprintf("escape='%s'", *c.Escape))
	}

	if c.Filename != nil {
		opts = append(opts, fmt.Sprintf("filename=%v", *c.Filename))
	}

	if c.ForceNotNull != nil && len(*c.ForceNotNull) > 0 {
		forceNotNullValues := strings.Join(*c.ForceNotNull, ",")
		opts = append(opts, fmt.Sprintf("force_not_null=%s", forceNotNullValues))
	}

	if c.Header != nil {
		opts = append(opts, fmt.Sprintf("header=%v", *c.Header))
	}

	if c.IgnoreErrors != nil {
		opts = append(opts, fmt.Sprintf("ignore_errors=%v", *c.IgnoreErrors))
	}

	if c.MaxLineSize != nil {
		opts = append(opts, fmt.Sprintf("max_line_size=%d", *c.MaxLineSize))
	}

	if c.NewLine != nil {
		opts = append(opts, fmt.Sprintf("new_line='%s'", *c.NewLine))
	}

	if c.NormalizeNames != nil {
		opts = append(opts, fmt.Sprintf("normalize_names=%v", *c.NormalizeNames))
	}

	if c.NullPadding != nil {
		opts = append(opts, fmt.Sprintf("null_padding=%v", *c.NullPadding))
	}

	if c.NullStr != nil {
		opts = append(opts, fmt.Sprintf("null_str='%s'", *c.NullStr))
	}

	if c.Quote != nil {
		opts = append(opts, fmt.Sprintf("quote='%s'", *c.Quote))
	}

	if c.SampleSize != nil {
		opts = append(opts, fmt.Sprintf("sample_size=%d", *c.SampleSize))
	}

	if c.TimestampFormat != nil {
		opts = append(opts, fmt.Sprintf("timestamp_format='%s'", *c.TimestampFormat))
	}

	return opts
}
