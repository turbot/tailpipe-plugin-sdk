package enrichment

type CommonFieldsMappings struct {
	// Mandatory fields
	TpID string `hcl:"tp_id"`
	//TpSourceType      *string `hcl:"tp_source_type"`
	//TpIngestTimestamp *string `hcl:"tp_ingest_timestamp"`
	TpTimestamp *string `hcl:"tp_timestamp"`

	// Hive fields
	//TpPartition *string `hcl:"tp_partition"`
	TpIndex *string `hcl:"tp_index"`
	TpDate  *string `hcl:"tp_date"`

	// Optional fields
	TpSourceIP       *string `hcl:"tp_source_ip"`
	TpDestinationIP  *string `hcl:"tp_destination_ip"`
	TpSourceName     *string `hcl:"tp_source_name"`
	TpSourceLocation *string `hcl:"tp_source_location"`

	// Searchable
	TpAkas      []*string `hcl:"tp_akas,omitempty"`
	TpIps       []*string `hcl:"tp_ips,omitempty"`
	TpTags      []*string `hcl:"tp_tags,omitempty"`
	TpDomains   []*string `hcl:"tp_domains,omitempty"`
	TpEmails    []*string `hcl:"tp_emails,omitempty"`
	TpUsernames []*string `hcl:"tp_usernames,omitempty"`
}
