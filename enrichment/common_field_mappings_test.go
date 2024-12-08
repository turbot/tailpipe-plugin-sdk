package enrichment

import (
	"github.com/turbot/pipe-fittings/utils"
	"reflect"
	"testing"
)

func TestCommonFieldsMappings_AsMap(t *testing.T) {
	type fields struct {
		TpTimestamp      string
		TpIndex          *string
		TpSourceIP       *string
		TpDestinationIP  *string
		TpSourceName     *string
		TpSourceLocation *string
		TpAkas           *string
		TpIps            *string
		TpTags           *string
		TpDomains        *string
		TpEmails         *string
		TpUsernames      *string
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]string
	}{
		{
			name: "Only non-pointer field set",
			fields: fields{
				TpTimestamp: "source_timestamp",
			},
			want: map[string]string{
				"tp_timestamp": "source_timestamp",
			},
		},
		{
			name: "Mix of non-pointer and non-nil pointer fields",
			fields: fields{
				TpTimestamp: "source_timestamp",
				TpIndex:     utils.ToPointer("source_index"),
				TpSourceIP:  utils.ToPointer("source_ip"),
			},
			want: map[string]string{
				"tp_timestamp": "source_timestamp",
				"tp_index":     "source_index",
				"tp_source_ip": "source_ip",
			},
		},
		{
			name: "All fields set",
			fields: fields{
				TpTimestamp:      "source_timestamp",
				TpIndex:          utils.ToPointer("source_index"),
				TpSourceIP:       utils.ToPointer("source_ip"),
				TpDestinationIP:  utils.ToPointer("source_destination_ip"),
				TpSourceName:     utils.ToPointer("source_name"),
				TpSourceLocation: utils.ToPointer("source_location"),
				TpAkas:           utils.ToPointer("source_akas"),
				TpIps:            utils.ToPointer("source_ips"),
				TpTags:           utils.ToPointer("source_tags"),
				TpDomains:        utils.ToPointer("source_domains"),
				TpEmails:         utils.ToPointer("source_emails"),
				TpUsernames:      utils.ToPointer("source_usernames"),
			},
			want: map[string]string{
				"tp_timestamp":       "source_timestamp",
				"tp_index":           "source_index",
				"tp_source_ip":       "source_ip",
				"tp_destination_ip":  "source_destination_ip",
				"tp_source_name":     "source_name",
				"tp_source_location": "source_location",
				"tp_akas":            "source_akas",
				"tp_ips":             "source_ips",
				"tp_tags":            "source_tags",
				"tp_domains":         "source_domains",
				"tp_emails":          "source_emails",
				"tp_usernames":       "source_usernames",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CommonFieldsMappings{
				TpTimestamp:      tt.fields.TpTimestamp,
				TpIndex:          tt.fields.TpIndex,
				TpSourceIP:       tt.fields.TpSourceIP,
				TpDestinationIP:  tt.fields.TpDestinationIP,
				TpSourceName:     tt.fields.TpSourceName,
				TpSourceLocation: tt.fields.TpSourceLocation,
				TpAkas:           tt.fields.TpAkas,
				TpIps:            tt.fields.TpIps,
				TpTags:           tt.fields.TpTags,
				TpDomains:        tt.fields.TpDomains,
				TpEmails:         tt.fields.TpEmails,
				TpUsernames:      tt.fields.TpUsernames,
			}
			if got := c.AsMap(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AsMap() = %v, want %v", got, tt.want)
			}
		})
	}
}
