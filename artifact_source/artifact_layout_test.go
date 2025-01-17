package artifact_source

import (
	"github.com/elastic/go-grok"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
	"reflect"
	"strings"
	"testing"
)

var patterns = map[string]string{
	"REGION": `[a-zA-Z0-9\-]+`,
}
var pattern = `AWSLogs/%{WORD:org}/%{WORD:account_id}/CloudTrail/%{REGION:region}/%{YEAR:year}/%{MONTHNUM:month}/%{MONTHDAY:day}/%{NOTSPACE:file_name}.%{WORD:ext}`
var org1_account1_regionuseast1_with_date = strings.Split(`AWSLogs/org1/1/CloudTrail/us-east-1/2024/01/15/logfile1.json`, "/")
var org1_account1 = strings.Split(`AWSLogs/org1/1/CloudTrail/logfile1.json`, "/")

// TODO test path too short,
// i.e. if pattern is AWSLogs/%{WORD:org}/%{WORD:account_id}/CloudTrail/%{NOTSPACE:region}/%{YEAR:year}/%{MONTHNUM:month}/%{MONTHDAY:day}/%{NOTSPACE:file_name}.%{WORD:ext}`
// this path should fail: AWSLogs/org1/1/CloudTrail/1_1.log

func Test_pathSegmentSatisfiesFilters(t *testing.T) {
	type args struct {
		pathSegment string
		isFile      bool
		fileLayout  string
		filters     []string
	}
	tests := []struct {
		name             string
		args             args
		wantMatch        bool
		expectedMetadata map[string][]byte
	}{
		// Single segment, positive case
		{
			name: "org1_account1_regionuseast1_with_date - 1 segment, filter not used",
			args: args{
				pathSegment: org1_account1_regionuseast1_with_date[0],
				fileLayout:  pattern,
				filters:     []string{"org = 'org1'"},
			},
			wantMatch:        true,
			expectedMetadata: map[string][]byte{},
		},
		// Single segment, negative case
		{
			name: "org1_account1_regionuseast1_with_date - 1 segment, filter not used",
			args: args{
				pathSegment: org1_account1_regionuseast1_with_date[0],
				fileLayout:  pattern,
				filters:     []string{"org != 'org1'"},
			},
			wantMatch:        true,
			expectedMetadata: map[string][]byte{},
		},
		// Two segments, positive case
		{
			name: "org1_account1_regionuseast1_with_date - 2 segments, matches filter",
			args: args{
				pathSegment: strings.Join(org1_account1_regionuseast1_with_date[:2], "/"),
				fileLayout:  pattern,
				filters:     []string{"org = 'org1'", "account_id = '1'"},
			},
			wantMatch:        true,
			expectedMetadata: map[string][]byte{"org": []byte("org1")},
		},
		// Two segments, negative case
		{
			name: "org1_account1_regionuseast1_with_date - 2 segments, org doesn't match filter",
			args: args{
				pathSegment: strings.Join(org1_account1_regionuseast1_with_date[:2], "/"),
				fileLayout:  pattern,
				filters:     []string{"org != 'org1'", "account_id = '1'"},
			},
			wantMatch:        false,
			expectedMetadata: map[string][]byte{"org": []byte("org1")},
		},

		// Three segments, positive case
		{
			name: "org1_account1_regionuseast1_with_date - 3 segments, matches filter",
			args: args{
				pathSegment: strings.Join(org1_account1_regionuseast1_with_date[:3], "/"),
				fileLayout:  pattern,
				filters:     []string{"org = 'org1'", "account_id = '1'", "region = 'us-east-1'"},
			},
			wantMatch:        true,
			expectedMetadata: map[string][]byte{"org": []byte("org1"), "account_id": []byte("1")},
		},
		// Three segments, negative case
		{
			name: "org1_account1_regionuseast1_with_date - 3 segments, org doesn't match filter",
			args: args{
				pathSegment: strings.Join(org1_account1_regionuseast1_with_date[:3], "/"),
				fileLayout:  pattern,
				filters:     []string{"org != 'org1'", "account_id = '1'"},
			},
			wantMatch:        false,
			expectedMetadata: map[string][]byte{"org": []byte("org1"), "account_id": []byte("1")},
		},
		{
			name: "org1_account1_regionuseast1_with_date - 3 segments, account doesn't match filter",
			args: args{
				pathSegment: strings.Join(org1_account1_regionuseast1_with_date[:3], "/"),
				fileLayout:  pattern,
				filters:     []string{"org = 'org1'", "account_id != '1'", "region = 'us-east-1'"},
			},
			wantMatch:        false,
			expectedMetadata: map[string][]byte{"org": []byte("org1"), "account_id": []byte("1")},
		},
		// Full path, positive case
		{
			name: "org1_account1_regionuseast1_with_date - full path, matches filter",
			args: args{
				pathSegment: strings.Join(org1_account1_regionuseast1_with_date, "/"),
				fileLayout:  pattern,
				isFile:      true,
				filters:     []string{"org = 'org1'", "account_id = '1'", "region = 'us-east-1'"},
			},
			wantMatch: true,
			expectedMetadata: map[string][]byte{
				"org":        []byte("org1"),
				"account_id": []byte("1"),
				"region":     []byte("us-east-1"),
				"year":       []byte("2024"),
				"month":      []byte("01"),
				"day":        []byte("15"),
				"file_name":  []byte("logfile1"),
				"ext":        []byte("json"),
			},
		},
		// Full path, negative cases
		{
			name: "org1_account1_regionuseast1_with_date - full path, doesn't match filter",
			args: args{
				pathSegment: strings.Join(org1_account1_regionuseast1_with_date, "/"),
				fileLayout:  pattern,
				isFile:      true,
				filters:     []string{"org != 'org1'", "account_id = '1'", "region = 'us-east-1'"},
			},
			wantMatch: false,
			expectedMetadata: map[string][]byte{
				"org":        []byte("org1"),
				"account_id": []byte("1"),
				"region":     []byte("us-east-1"),
				"year":       []byte("2024"),
				"month":      []byte("01"),
				"day":        []byte("15"),
				"file_name":  []byte("logfile1"),
				"ext":        []byte("json"),
			},
		},
		{
			name: "org1_account1_regionuseast1_with_date - full path, region doesn't match filter",
			args: args{
				pathSegment: strings.Join(org1_account1_regionuseast1_with_date, "/"),
				fileLayout:  pattern,
				isFile:      true,
				filters:     []string{"org = 'org1'", "account_id = '1'", "region != 'us-east-1'"},
			},
			wantMatch: false,
			expectedMetadata: map[string][]byte{
				"org":        []byte("org1"),
				"account_id": []byte("1"),
				"region":     []byte("us-east-1"),
				"year":       []byte("2024"),
				"month":      []byte("01"),
				"day":        []byte("15"),
				"file_name":  []byte("logfile1"),
				"ext":        []byte("json"),
			},
		},

		// Invalid path, negative case
		{
			name: "org1_account1 - full path, missing segments should fail",
			args: args{
				pathSegment: strings.Join(org1_account1, "/"),
				fileLayout:  pattern,
				isFile:      true,
			},
			wantMatch:        false,
			expectedMetadata: map[string][]byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := grok.New()
			// Add patterns
			if err := g.AddPatterns(patterns); err != nil {
				t.Fatalf("failed to add patterns: %v", err)
			}

			// Parse filters
			filters, err := helpers.BuildFilterMap(tt.args.filters)
			if err != nil {
				t.Fatalf("failed to parse filters: %v", err)
			}
			var metadata map[string][]byte
			var match bool
			if tt.args.isFile {
				match, metadata, err = getPathLeafMetadata(g, tt.args.pathSegment, tt.args.fileLayout)
			} else {
				match, metadata, err = getPathSegmentMetadata(g, tt.args.pathSegment, tt.args.fileLayout)
			}
			if err != nil {
				t.Fatalf("failed to extract metadata: %v", err)
			}

			// if the pattern match fails but we wanted a match
			if !match && tt.wantMatch {
				t.Errorf("match = %v, wantMatch %v", match, tt.wantMatch)
			}

			// Compare metadata
			for key, expectedValue := range tt.expectedMetadata {
				if value, exists := metadata[key]; !exists || string(value) != string(expectedValue) {
					t.Errorf("metadata[%s] = %v, wantMatch %v", key, value, expectedValue)
				}
			}
			// if we have metadata and filters, check if the metadata satisfies the filters
			if len(metadata) > 0 {
				if got := metadataSatisfiesFilters(ByteMapToStringMap(metadata), filters); got != tt.wantMatch {
					t.Errorf("metadataSatisfiesFilters() = %v, wantMatch %v", got, tt.wantMatch)
				}
			}

		})
	}
}
func Test_expandPatternIntoOptionalAlternatives(t *testing.T) {
	type args struct {
		pattern string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "AWSLogs with one optional segment",
			args: args{
				pattern: "AWSLogs/(%{WORD:org}/)?%{WORD:account_id}/Cloud/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
			want: []string{
				"AWSLogs/%{WORD:org}/%{WORD:account_id}/Cloud/%{NOTSPACE:file_name}.%{WORD:ext}",
				"AWSLogs/%{WORD:account_id}/Cloud/%{NOTSPACE:file_name}.%{WORD:ext}",
			},
		},
		{
			name: "GCPLogs with two optional segments",
			args: args{
				pattern: "GCPLogs/(%{WORD:project_id}/)?(%{WORD:zone}/)?Instances/%{NOTSPACE:instance_id}.%{WORD:ext}",
			},
			want: []string{
				"GCPLogs/%{WORD:project_id}/%{WORD:zone}/Instances/%{NOTSPACE:instance_id}.%{WORD:ext}",
				"GCPLogs/%{WORD:project_id}/Instances/%{NOTSPACE:instance_id}.%{WORD:ext}",
				"GCPLogs/%{WORD:zone}/Instances/%{NOTSPACE:instance_id}.%{WORD:ext}",
				"GCPLogs/Instances/%{NOTSPACE:instance_id}.%{WORD:ext}",
			},
		},
		{
			name: "Logs with three optional segments",
			args: args{
				pattern: "Logs/(%{WORD:tenant_id}/)?(%{WORD:service}/)?(%{WORD:log_type}/)?%{NOTSPACE:log_name}.%{WORD:ext}",
			},
			want: []string{
				"Logs/%{WORD:tenant_id}/%{WORD:service}/%{WORD:log_type}/%{NOTSPACE:log_name}.%{WORD:ext}",
				"Logs/%{WORD:tenant_id}/%{WORD:service}/%{NOTSPACE:log_name}.%{WORD:ext}",
				"Logs/%{WORD:tenant_id}/%{WORD:log_type}/%{NOTSPACE:log_name}.%{WORD:ext}",
				"Logs/%{WORD:tenant_id}/%{NOTSPACE:log_name}.%{WORD:ext}",
				"Logs/%{WORD:service}/%{WORD:log_type}/%{NOTSPACE:log_name}.%{WORD:ext}",
				"Logs/%{WORD:service}/%{NOTSPACE:log_name}.%{WORD:ext}",
				"Logs/%{WORD:log_type}/%{NOTSPACE:log_name}.%{WORD:ext}",
				"Logs/%{NOTSPACE:log_name}.%{WORD:ext}",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExpandPatternIntoOptionalAlternatives(tt.args.pattern); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExpandPatternIntoOptionalAlternatives() = %v, want %v", got, tt.want)
			}
		})
	}
}
