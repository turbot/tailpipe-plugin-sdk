package enrichment

import "reflect"

const DefaultIndex = "default"

type CommonFieldsMappings struct {
	// Mandatory fields
	//TpID string `hcl:"tp_id"`
	//TpSourceType      *string `hcl:"tp_source_type"`
	//TpIngestTimestamp *string `hcl:"tp_ingest_timestamp"`
	TpTimestamp string `hcl:"tp_timestamp"`

	// Hive fields
	//TpPartition *string `hcl:"tp_partition"`
	TpIndex *string `hcl:"tp_index"`
	//TpDate  *string `hcl:"tp_date"`

	// Optional fields
	TpSourceIP       *string `hcl:"tp_source_ip"`
	TpDestinationIP  *string `hcl:"tp_destination_ip"`
	TpSourceName     *string `hcl:"tp_source_name"`
	TpSourceLocation *string `hcl:"tp_source_location"`

	// Searchable
	TpAkas      *string `hcl:"tp_akas"`
	TpIps       *string `hcl:"tp_ips"`
	TpTags      *string `hcl:"tp_tags"`
	TpDomains   *string `hcl:"tp_domains"`
	TpEmails    *string `hcl:"tp_emails"`
	TpUsernames *string `hcl:"tp_usernames"`
}

// AsMap converts the fields of the struct to a map, using their "hcl" tags as keys.
// For pointers, includes the dereferenced value if non-nil. For non-pointers, includes the value.
func (c *CommonFieldsMappings) AsMap() map[string]string {
	result := make(map[string]string)

	// Use reflection to iterate over the struct fields

	// Get the value the receiver pointer points to
	v := reflect.ValueOf(c).Elem()
	// Get the type of the struct
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		// Get the "hcl" tag
		tag := field.Tag.Get("hcl")
		if tag == "" || tag == "-" {
			continue // Skip fields without a tag or explicitly ignored
		}

		// Handle pointer and non-pointer fields
		if value.Kind() == reflect.Ptr {
			// For pointers, include only non-nil values
			if !value.IsNil() {
				result[tag] = value.Elem().Interface().(string) // Dereference the pointer
			}
		} else {
			// For non-pointer fields, include the value directly
			result[tag] = value.Interface().(string)
		}
	}
	return result
}
