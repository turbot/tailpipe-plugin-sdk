package schema

import (
	"fmt"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/utils"
)

const DefaultIndex = "default"

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
		missingFields = append(missingFields, "TpID")
	}
	if c.TpSourceType == "" {
		missingFields = append(missingFields, "TpSourceType")
	}
	if c.TpIngestTimestamp.IsZero() {
		missingFields = append(missingFields, "TpIngestTimestamp")
	}
	if c.TpTimestamp.IsZero() {
		missingFields = append(missingFields, "TpTimestamp")
	}
	if c.TpTable == "" {
		missingFields = append(missingFields, "TpTable")
	}
	if c.TpPartition == "" {
		missingFields = append(missingFields, "TpPartition")
	}
	if c.TpIndex == "" {
		missingFields = append(missingFields, "TpIndex")
	} else {
		// handles instances where tp_index is the same value with different casing (as seen on Azure data with subscription_id being either upper or lower case)
		// when tp_index is differential in casing it causes data to not be set against the partition correctly
		c.TpIndex = strings.ToLower(c.TpIndex)
	}
	if c.TpDate.IsZero() {
		missingFields = append(missingFields, "TpDate")
	}
	// verify that the date is a date and not a datetime
	if !c.TpDate.Equal(c.TpDate.Truncate(24 * time.Hour)) {
		invalidFields = append(invalidFields, "TpDate")
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

// GetCommonFields implements RowStruct
func (c *CommonFields) GetCommonFields() CommonFields {
	// just return ourselves
	return *c
}

// InitialiseFromMap initializes a CommonFields struct using a source map
func (c *CommonFields) InitialiseFromMap(source map[string]string) {
	const timeFormat = time.RFC3339

	// Mandatory fields
	if value, ok := source["tp_id"]; ok {
		c.TpID = value
	}
	if value, ok := source["tp_source_type"]; ok {
		c.TpSourceType = value
	}
	if value, ok := source["tp_ingest_timestamp"]; ok {
		if t, err := time.Parse(timeFormat, value); err == nil {
			c.TpIngestTimestamp = t
		}
	}
	if value, ok := source["tp_timestamp"]; ok {
		if t, err := time.Parse(timeFormat, value); err == nil {
			c.TpTimestamp = t
		}
	}

	// Hive fields
	if value, ok := source["tp_table"]; ok {
		c.TpTable = value
	}
	if value, ok := source["tp_partition"]; ok {
		c.TpPartition = value
	}
	if value, ok := source["tp_index"]; ok {
		c.TpIndex = value
	}
	if value, ok := source["tp_date"]; ok {
		if t, err := time.Parse(timeFormat, value); err == nil {
			c.TpDate = t
		}
	}

	// Optional fields
	if value, ok := source["tp_source_ip"]; ok {
		c.TpSourceIP = &value
	}
	if value, ok := source["tp_destination_ip"]; ok {
		c.TpDestinationIP = &value
	}
	if value, ok := source["tp_source_name"]; ok {
		c.TpSourceName = &value
	}
	if value, ok := source["tp_source_location"]; ok {
		c.TpSourceLocation = &value
	}

	// Searchable fields (slices)
	if value, ok := source["tp_akas"]; ok {
		c.TpAkas = strings.Split(value, ",")
	}
	if value, ok := source["tp_ips"]; ok {
		c.TpIps = strings.Split(value, ",")
	}
	if value, ok := source["tp_tags"]; ok {
		c.TpTags = strings.Split(value, ",")
	}
	if value, ok := source["tp_domains"]; ok {
		c.TpDomains = strings.Split(value, ",")
	}
	if value, ok := source["tp_emails"]; ok {
		c.TpEmails = strings.Split(value, ",")
	}
	if value, ok := source["tp_usernames"]; ok {
		c.TpUsernames = strings.Split(value, ",")
	}
}

// AsMap converts the CommonFields struct into a map[string]string.
func (c *CommonFields) AsMap() map[string]string {
	result := make(map[string]string)
	const timeFormat = time.RFC3339

	// Mandatory fields
	result["tp_id"] = c.TpID
	result["tp_source_type"] = c.TpSourceType
	result["tp_ingest_timestamp"] = c.TpIngestTimestamp.Format(timeFormat)
	result["tp_timestamp"] = c.TpTimestamp.Format(timeFormat)

	// Hive fields
	result["tp_table"] = c.TpTable
	result["tp_partition"] = c.TpPartition
	result["tp_index"] = c.TpIndex
	result["tp_date"] = c.TpDate.Format(timeFormat)

	// Optional fields
	if c.TpSourceIP != nil {
		result["tp_source_ip"] = *c.TpSourceIP
	}
	if c.TpDestinationIP != nil {
		result["tp_destination_ip"] = *c.TpDestinationIP
	}
	if c.TpSourceName != nil {
		result["tp_source_name"] = *c.TpSourceName
	}
	if c.TpSourceLocation != nil {
		result["tp_source_location"] = *c.TpSourceLocation
	}

	// Searchable fields
	if len(c.TpAkas) > 0 {
		result["tp_akas"] = strings.Join(c.TpAkas, ",")
	}
	if len(c.TpIps) > 0 {
		result["tp_ips"] = strings.Join(c.TpIps, ",")
	}
	if len(c.TpTags) > 0 {
		result["tp_tags"] = strings.Join(c.TpTags, ",")
	}
	if len(c.TpDomains) > 0 {
		result["tp_domains"] = strings.Join(c.TpDomains, ",")
	}
	if len(c.TpEmails) > 0 {
		result["tp_emails"] = strings.Join(c.TpEmails, ",")
	}
	if len(c.TpUsernames) > 0 {
		result["tp_usernames"] = strings.Join(c.TpUsernames, ",")
	}

	return result
}

// TODO improve these descriptions https://github.com/turbot/tailpipe-plugin-sdk/issues/83
func DefaultCommonFieldDescriptions() map[string]string {
	return map[string]string{
		"tp_id":               "A unique identifier for the row.",
		"tp_source_type":      "The name of the source that collected the row.",
		"tp_ingest_timestamp": "The timestamp in UTC when the row was ingested into the system.",
		"tp_timestamp":        "The original timestamp in UTC when the event or log entry was generated.",
		"tp_table":            "The name of the table.",
		"tp_partition":        "The name of the partition as defined in the Tailpipe configuration file.",
		"tp_index":            "The name of the optional index used to partition the data.",
		"tp_date":             "The original date when the event or log entry was generated in YYYY-MM-DD format.",
		"tp_source_ip":        "The IP address of the source.",
		"tp_destination_ip":   "The IP address of the destination.",
		"tp_source_name":      "The name or identifier of the source generating the row, such as a service name.",
		"tp_source_location":  "The geographic or network location of the source, such as a region.",
		"tp_akas":             "A list of associated globally unique identifier strings (also known as).",
		"tp_ips":              "A list of associated IP addresses.",
		"tp_tags":             "A list of associated tags or labels.",
		"tp_domains":          "A list of associated domain names.",
		"tp_emails":           "A list of associated email addresses.",
		"tp_usernames":        "A list of associated usernames or identities.",
	}
}

func IsCommonField(name string) bool {
	_, ok := DefaultCommonFieldDescriptions()[name]
	return ok
}
