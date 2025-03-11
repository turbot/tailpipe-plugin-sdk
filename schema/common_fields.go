package schema

import (
	"fmt"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
)

const DefaultIndex = "default"

// CommonFieldsSchema is the TableSchema for the common fields
// it is used for custom tables
func CommonFieldsSchema() *TableSchema {
	return &TableSchema{
		Name: "common_fields",
		Columns: []*ColumnSchema{
			{
				ColumnName:  constants.TpTimestamp,
				SourceName:  constants.TpTimestamp,
				Type:        "TIMESTAMP",
				Description: DefaultCommonFieldDescriptions[constants.TpTimestamp],
				Required:    true,
			},
			{
				ColumnName:  constants.TpID,
				SourceName:  constants.TpID,
				Type:        "VARCHAR",
				Description: DefaultCommonFieldDescriptions[constants.TpID],
				Required:    true,
			},
			{
				ColumnName:  constants.TpSourceType,
				SourceName:  constants.TpSourceType,
				Type:        "VARCHAR",
				Description: DefaultCommonFieldDescriptions[constants.TpSourceType],
			},
			{
				ColumnName:  constants.TpIngestTimestamp,
				SourceName:  constants.TpIngestTimestamp,
				Type:        "TIMESTAMP",
				Description: DefaultCommonFieldDescriptions[constants.TpIngestTimestamp],
			},
			// Hive fields
			{
				ColumnName:  constants.TpTable,
				SourceName:  constants.TpTable,
				Type:        "VARCHAR",
				Description: DefaultCommonFieldDescriptions[constants.TpTable],
			},
			{
				ColumnName:  constants.TpPartition,
				SourceName:  constants.TpPartition,
				Type:        "VARCHAR",
				Description: DefaultCommonFieldDescriptions[constants.TpPartition],
			},
			{
				ColumnName:  constants.TpIndex,
				SourceName:  constants.TpIndex,
				Type:        "VARCHAR",
				Description: DefaultCommonFieldDescriptions[constants.TpIndex],
			},
			{
				ColumnName:  constants.TpDate,
				SourceName:  constants.TpDate,
				Type:        "DATE",
				Description: DefaultCommonFieldDescriptions[constants.TpDate],
			},
			// Optional fields
			{
				ColumnName:  constants.TpSourceIP,
				SourceName:  constants.TpSourceIP,
				Type:        "VARCHAR",
				Description: DefaultCommonFieldDescriptions[constants.TpSourceIP],
			},
			{
				ColumnName:  constants.TpDestinationIP,
				SourceName:  constants.TpDestinationIP,
				Type:        "VARCHAR",
				Description: DefaultCommonFieldDescriptions[constants.TpDestinationIP],
			},
			{
				ColumnName:  constants.TpSourceName,
				SourceName:  constants.TpSourceName,
				Type:        "VARCHAR",
				Description: DefaultCommonFieldDescriptions[constants.TpSourceName],
			},
			{
				ColumnName:  constants.TpSourceLocation,
				SourceName:  constants.TpSourceLocation,
				Type:        "VARCHAR",
				Description: DefaultCommonFieldDescriptions[constants.TpSourceLocation],
			},
			// Searchable fields (arrays)
			{
				ColumnName:  constants.TpAkas,
				SourceName:  constants.TpAkas,
				Type:        "VARCHAR[]",
				Description: DefaultCommonFieldDescriptions[constants.TpAkas],
			},
			{
				ColumnName:  constants.TpIps,
				SourceName:  constants.TpIps,
				Type:        "VARCHAR[]",
				Description: DefaultCommonFieldDescriptions[constants.TpIps],
			},
			{
				ColumnName:  constants.TpTags,
				SourceName:  constants.TpTags,
				Type:        "VARCHAR[]",
				Description: DefaultCommonFieldDescriptions[constants.TpTags],
			},
			{
				ColumnName:  constants.TpDomains,
				SourceName:  constants.TpDomains,
				Type:        "VARCHAR[]",
				Description: DefaultCommonFieldDescriptions[constants.TpDomains],
			},
			{
				ColumnName:  constants.TpEmails,
				SourceName:  constants.TpEmails,
				Type:        "VARCHAR[]",
				Description: DefaultCommonFieldDescriptions[constants.TpEmails],
			},
			{
				ColumnName:  constants.TpUsernames,
				SourceName:  constants.TpUsernames,
				Type:        "VARCHAR[]",
				Description: DefaultCommonFieldDescriptions[constants.TpUsernames],
			},
		},
	}
}

// CommonFields represents the common fields with JSON tags
type CommonFields struct {
	// Mandatory fields
	TpID              string    `json:"tp_id"`
	TpSourceType      string    `json:"tp_source_type"`
	TpIngestTimestamp time.Time `json:"tp_ingest_timestamp"`
	TpTimestamp       time.Time `json:"tp_timestamp"`

	// Hive fields
	TpTable     string    `json:"tp_table"`
	TpPartition string    `json:"tp_partition"`
	TpIndex     string    `json:"tp_index"`
	TpDate      time.Time `json:"tp_date" parquet:"type=DATE"`

	// Optional fields
	TpSourceIP       *string `json:"tp_source_ip"`
	TpDestinationIP  *string `json:"tp_destination_ip"`
	TpSourceName     *string `json:"tp_source_name"`
	TpSourceLocation *string `json:"tp_source_location"`

	// Searchable
	TpAkas      []string `json:"tp_akas,omitempty"`
	TpIps       []string `json:"tp_ips,omitempty"`
	TpTags      []string `json:"tp_tags,omitempty"`
	TpDomains   []string `json:"tp_domains,omitempty"`
	TpEmails    []string `json:"tp_emails,omitempty"`
	TpUsernames []string `json:"tp_usernames,omitempty"`
}

// Validate implements the Validatable interface and is used to validate that the required fields have been set
// it can also be overridden by RowStruct implementations to perform additional validation - in this case
// CommonFields.Validate() should be called first
func (c *CommonFields) Validate() error {
	var missingFields []string
	var invalidFields []string
	// ensure required fields are set
	if c.TpID == "" {
		missingFields = append(missingFields, constants.TpID)
	}
	if c.TpSourceType == "" {
		missingFields = append(missingFields, constants.TpSourceType)
	}
	if c.TpIngestTimestamp.IsZero() {
		missingFields = append(missingFields, constants.TpIngestTimestamp)
	}
	if c.TpTimestamp.IsZero() {
		missingFields = append(missingFields, constants.TpTimestamp)
	}
	if c.TpTable == "" {
		missingFields = append(missingFields, constants.TpTable)
	}
	if c.TpPartition == "" {
		missingFields = append(missingFields, constants.TpPartition)
	}
	if c.TpIndex == "" {
		missingFields = append(missingFields, constants.TpIndex)
	} else {
		// handles instances where tp_index is the same value with different casing (as seen on Azure data with subscription_id being either upper or lower case)
		// when tp_index is differential in casing it causes data to not be set against the partition correctly
		c.TpIndex = strings.ToLower(c.TpIndex)
	}
	if c.TpDate.IsZero() {
		missingFields = append(missingFields, constants.TpDate)
	}
	// verify that the date is a date and not a datetime
	if !c.TpDate.Equal(c.TpDate.Truncate(24 * time.Hour)) {
		invalidFields = append(invalidFields, constants.TpDate)
	}
	var missingFieldsStr, invalidFieldsStr string
	if len(missingFields) > 0 {
		missingFieldsStr = fmt.Sprintf("missing required %s: %s", utils.Pluralize("field", len(missingFields)), strings.Join(missingFields, ", "))
	}
	if len(invalidFields) > 0 {
		invalidFieldsStr = fmt.Sprintf("invalid fields: %s", strings.Join(invalidFields, ", "))
	}
	// Concatenate the messages without extra spaces
	errorMsg := missingFieldsStr
	if missingFieldsStr != "" && invalidFieldsStr != "" {
		errorMsg += " "
	}
	errorMsg += invalidFieldsStr

	if errorMsg != "" {
		return fmt.Errorf("row validation failed: %s", errorMsg)
	}
	return nil
}

// InitialiseFromMap initializes a CommonFields struct using a source map
func (c *CommonFields) InitialiseFromMap(source map[string]string) {
	const timeFormat = time.RFC3339

	// Mandatory fields
	if value, ok := source[constants.TpID]; ok {
		c.TpID = value
	}
	if value, ok := source[constants.TpSourceType]; ok {
		c.TpSourceType = value
	}
	if value, ok := source[constants.TpIngestTimestamp]; ok {
		if t, err := time.Parse(timeFormat, value); err == nil {
			c.TpIngestTimestamp = t
		}
	}
	if value, ok := source[constants.TpTimestamp]; ok {
		if t, err := time.Parse(timeFormat, value); err == nil {
			c.TpTimestamp = t
		}
	}

	// Hive fields
	if value, ok := source[constants.TpTable]; ok {
		c.TpTable = value
	}
	if value, ok := source[constants.TpPartition]; ok {
		c.TpPartition = value
	}
	if value, ok := source[constants.TpIndex]; ok {
		c.TpIndex = value
	}
	if value, ok := source[constants.TpDate]; ok {
		if t, err := time.Parse(timeFormat, value); err == nil {
			c.TpDate = t
		}
	}

	// Optional fields
	if value, ok := source[constants.TpSourceIP]; ok {
		c.TpSourceIP = &value
	}
	if value, ok := source[constants.TpDestinationIP]; ok {
		c.TpDestinationIP = &value
	}
	if value, ok := source[constants.TpSourceName]; ok {
		c.TpSourceName = &value
	}
	if value, ok := source[constants.TpSourceLocation]; ok {
		c.TpSourceLocation = &value
	}

	// Searchable fields (slices)
	if value, ok := source[constants.TpAkas]; ok {
		c.TpAkas = strings.Split(value, ",")
	}
	if value, ok := source[constants.TpIps]; ok {
		c.TpIps = strings.Split(value, ",")
	}
	if value, ok := source[constants.TpTags]; ok {
		c.TpTags = strings.Split(value, ",")
	}
	if value, ok := source[constants.TpDomains]; ok {
		c.TpDomains = strings.Split(value, ",")
	}
	if value, ok := source[constants.TpEmails]; ok {
		c.TpEmails = strings.Split(value, ",")
	}
	if value, ok := source[constants.TpUsernames]; ok {
		c.TpUsernames = strings.Split(value, ",")
	}
}

// AsMap converts the CommonFields struct into a map[string]string.
func (c *CommonFields) AsMap() map[string]string {
	result := make(map[string]string)
	const timeFormat = time.RFC3339

	// Mandatory fields
	result[constants.TpID] = c.TpID
	result[constants.TpSourceType] = c.TpSourceType
	result[constants.TpIngestTimestamp] = c.TpIngestTimestamp.Format(timeFormat)
	result[constants.TpTimestamp] = c.TpTimestamp.Format(timeFormat)

	// Hive fields
	result[constants.TpTable] = c.TpTable
	result[constants.TpPartition] = c.TpPartition
	result[constants.TpIndex] = c.TpIndex
	result[constants.TpDate] = c.TpDate.Format(timeFormat)

	// Optional fields
	if c.TpSourceIP != nil {
		result[constants.TpSourceIP] = *c.TpSourceIP
	}
	if c.TpDestinationIP != nil {
		result[constants.TpDestinationIP] = *c.TpDestinationIP
	}
	if c.TpSourceName != nil {
		result[constants.TpSourceName] = *c.TpSourceName
	}
	if c.TpSourceLocation != nil {
		result[constants.TpSourceLocation] = *c.TpSourceLocation
	}

	// Searchable fields
	if len(c.TpAkas) > 0 {
		result[constants.TpAkas] = strings.Join(c.TpAkas, ",")
	}
	if len(c.TpIps) > 0 {
		result[constants.TpIps] = strings.Join(c.TpIps, ",")
	}
	if len(c.TpTags) > 0 {
		result[constants.TpTags] = strings.Join(c.TpTags, ",")
	}
	if len(c.TpDomains) > 0 {
		result[constants.TpDomains] = strings.Join(c.TpDomains, ",")
	}
	if len(c.TpEmails) > 0 {
		result[constants.TpEmails] = strings.Join(c.TpEmails, ",")
	}
	if len(c.TpUsernames) > 0 {
		result[constants.TpUsernames] = strings.Join(c.TpUsernames, ",")
	}

	return result
}

// TODO improve these descriptions https://github.com/turbot/tailpipe-plugin-sdk/issues/83
var DefaultCommonFieldDescriptions = map[string]string{
	constants.TpID:              "A unique identifier for the row.",
	constants.TpSourceType:      "The name of the source that collected the row.",
	constants.TpIngestTimestamp: "The timestamp in UTC when the row was ingested into the system.",
	constants.TpTimestamp:       "The original timestamp in UTC when the event or log entry was generated.",
	constants.TpTable:           "The name of the table.",
	constants.TpPartition:       "The name of the partition as defined in the Tailpipe configuration file.",
	constants.TpIndex:           "The name of the optional index used to partition the data.",
	constants.TpDate:            "The original date when the event or log entry was generated in YYYY-MM-DD format.",
	constants.TpSourceIP:        "The IP address of the source.",
	constants.TpDestinationIP:   "The IP address of the destination.",
	constants.TpSourceName:      "The name or identifier of the source generating the row, such as a service name.",
	constants.TpSourceLocation:  "The geographic or network location of the source, such as a region.",
	constants.TpAkas:            "A list of associated globally unique identifier strings (also known as).",
	constants.TpIps:             "A list of associated IP addresses.",
	constants.TpTags:            "A list of associated tags or labels.",
	constants.TpDomains:         "A list of associated domain names.",
	constants.TpEmails:          "A list of associated email addresses.",
	constants.TpUsernames:       "A list of associated usernames or identities.",
}

func IsCommonField(name string) bool {
	_, ok := DefaultCommonFieldDescriptions[name]
	return ok
}
