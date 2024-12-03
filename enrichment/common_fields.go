package enrichment

import (
	"fmt"
	"strings"
	"time"
)

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
func (c CommonFields) Validate() error {
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
		missingFieldsStr = fmt.Sprintf("missing required fields: %s", strings.Join(missingFields, ", "))
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
		return fmt.Errorf("%s", errorMsg)
	}
	return nil
}

// GetCommonFields implements RowStruct
func (c CommonFields) GetCommonFields() CommonFields {
	// just return ourselves
	return c
}
