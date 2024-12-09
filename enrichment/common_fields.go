package enrichment

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/utils"
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
		return fmt.Errorf("%s", errorMsg)
	}
	return nil
}

// GetCommonFields implements RowStruct
func (c *CommonFields) GetCommonFields() CommonFields {
	// just return ourselves
	return *c
}

func (c *CommonFields) InitialiseFromMap(source map[string]string, mappings CommonFieldsMappings) {
	if val, ok := source[mappings.TpTimestamp]; ok {
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			c.TpTimestamp = t
			c.TpDate = t.Truncate(24 * time.Hour)
		} else {
			// TODO #errors what to do
			slog.Warn("Failed to parse timestamp", "value", val, "error", err)
		}
	}

	if mappings.TpIndex != nil {
		if val, ok := source[*mappings.TpIndex]; ok {
			c.TpIndex = val
		}
	}

	if mappings.TpSourceIP != nil {
		if val, ok := source[*mappings.TpSourceIP]; ok {
			c.TpSourceIP = &val
		}
	}

	if mappings.TpDestinationIP != nil {
		if val, ok := source[*mappings.TpDestinationIP]; ok {
			c.TpDestinationIP = &val

		}
	}

	if mappings.TpSourceName != nil {
		if val, ok := source[*mappings.TpSourceName]; ok {
			c.TpSourceName = &val
		}
	}

	if mappings.TpSourceLocation != nil {
		if val, ok := source[*mappings.TpSourceLocation]; ok {
			c.TpSourceLocation = &val
		}
	}

	// TODO K not sure about the arrays - for now assume escaped csv
	if mappings.TpAkas != nil {
		if val, ok := source[*mappings.TpAkas]; ok {
			// try to split the strings into an array
			c.TpAkas = strings.Split(val, ",")
		}
	}

	if mappings.TpIps != nil {
		if val, ok := source[*mappings.TpIps]; ok {
			// try to split the strings into an array
			c.TpIps = strings.Split(val, ",")
		}
	}

	if mappings.TpTags != nil {
		if val, ok := source[*mappings.TpTags]; ok {
			// try to split the strings into an array
			c.TpTags = strings.Split(val, ",")
		}
	}

	if mappings.TpDomains != nil {
		if val, ok := source[*mappings.TpDomains]; ok {
			// try to split the strings into an array
			c.TpDomains = strings.Split(val, ",")
		}
	}

	if mappings.TpEmails != nil {
		if val, ok := source[*mappings.TpEmails]; ok {
			// try to split the strings into an array
			c.TpEmails = strings.Split(val, ",")
		}
	}

	if mappings.TpUsernames != nil {
		if val, ok := source[*mappings.TpUsernames]; ok {
			// try to split the strings into an array
			c.TpUsernames = strings.Split(val, ",")
		}
	}

}

func (c *CommonFields) AsMap() map[string]interface{} {
	var m = make(map[string]interface{})
	m["tp_id"] = c.TpID
	m["tp_source_type"] = c.TpSourceType
	m["tp_ingest_timestamp"] = c.TpIngestTimestamp
	m["tp_timestamp"] = c.TpTimestamp
	m["tp_table"] = c.TpTable
	m["tp_partition"] = c.TpPartition
	m["tp_index"] = c.TpIndex
	m["tp_date"] = c.TpDate
	if c.TpSourceIP != nil {
		m["tp_source_ip"] = *c.TpSourceIP
	}
	if c.TpDestinationIP != nil {
		m["tp_destination_ip"] = *c.TpDestinationIP
	}
	if c.TpSourceName != nil {
		m["tp_source_name"] = *c.TpSourceName
	}
	if c.TpSourceLocation != nil {
		m["tp_source_location"] = *c.TpSourceLocation
	}

	m["tp_akas"] = c.TpAkas
	m["tp_ips"] = c.TpIps
	m["tp_tags"] = c.TpTags
	m["tp_domains"] = c.TpDomains
	m["tp_emails"] = c.TpEmails
	m["tp_usernames"] = c.TpUsernames

	return m
}
