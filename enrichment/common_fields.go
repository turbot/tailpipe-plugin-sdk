package enrichment

import (
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
	TpPartition string `json:"tp_partition"`
	TpIndex     string `json:"tp_index"`
	TpDate      string `json:"tp_date"`

	// Optional fields

	TpSourceIP       *string `json:"tp_source_ip"`
	TpDestinationIP  *string `json:"tp_destination_ip"`
	TpSourceName     string  `json:"tp_source_name"`
	TpSourceLocation *string `json:"tp_source_location"`

	// Searchable
	TpAkas      []string `json:"tp_akas,omitempty"`
	TpIps       []string `json:"tp_ips,omitempty"`
	TpTags      []string `json:"tp_tags,omitempty"`
	TpDomains   []string `json:"tp_domains,omitempty"`
	TpEmails    []string `json:"tp_emails,omitempty"`
	TpUsernames []string `json:"tp_usernames,omitempty"`
}

func (c *CommonFields) Clone() *CommonFields {
	if c == nil {
		return &CommonFields{}
	}

	return &CommonFields{
		TpID:              c.TpID,
		TpSourceType:      c.TpSourceType,
		TpSourceName:      c.TpSourceName,
		TpSourceLocation:  c.TpSourceLocation,
		TpIngestTimestamp: c.TpIngestTimestamp,
		TpTimestamp:       c.TpTimestamp,
		TpSourceIP:        c.TpSourceIP,
		TpDestinationIP:   c.TpDestinationIP,
		TpPartition:       c.TpPartition,
		TpIndex:           c.TpIndex,
		TpDate:            c.TpDate,
		TpAkas:            append([]string(nil), c.TpAkas...),
		TpIps:             append([]string(nil), c.TpIps...),
		TpTags:            append([]string(nil), c.TpTags...),
		TpDomains:         append([]string(nil), c.TpDomains...),
		TpEmails:          append([]string(nil), c.TpEmails...),
		TpUsernames:       append([]string(nil), c.TpUsernames...),
	}
}
