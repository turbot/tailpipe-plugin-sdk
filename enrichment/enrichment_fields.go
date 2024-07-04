package enrichment

import "github.com/turbot/tailpipe-plugin-sdk/helpers"

// CommonFields represents the common fields with JSON tags
// NOTE: DO NOT use omitEmpty tags in this struct - otherwise the parquet conversion fails as the schema is not complete
type CommonFields struct {
	TpID              string             `json:"tp_id"`
	TpSourceType      string             `json:"tp_source_type"`
	TpSourceName      string             `json:"tp_source_name"`
	TpSourceLocation  *string            `json:"tp_source_location"`
	TpIngestTimestamp helpers.UnixMillis `json:"tp_ingest_timestamp"`

	// Standardized
	TpTimestamp     helpers.UnixMillis `json:"tp_timestamp"`
	TpSourceIP      *string            `json:"tp_source_ip"`
	TpDestinationIP *string            `json:"tp_destination_ip"`

	// Hive fields
	TpCollection string `json:"tp_collection"`
	TpConnection string `json:"tp_connection"`
	TpYear       int32  `json:"tp_year"`
	TpMonth      int32  `json:"tp_month"`
	TpDay        int32  `json:"tp_day"`

	// Searchable
	TpAkas      []string `json:"tp_akas"`
	TpIps       []string `json:"tp_ips"`
	TpTags      []string `json:"tp_tags"`
	TpDomains   []string `json:"tp_domains"`
	TpEmails    []string `json:"tp_emails"`
	TpUsernames []string `json:"tp_usernames"`
}
