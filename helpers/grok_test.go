package helpers

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExtractNamedGroupsFromGrok(t *testing.T) {
	type args struct {
		grokPattern string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Simple pattern with one named group",
			args: args{
				grokPattern: `%{WORD:field1}`,
			},
			want: []string{"field1"},
		},
		{
			name: "Pattern with multiple named groups",
			args: args{
				grokPattern: `%{WORD:field1}/%{NUMBER:field2}/%{NOTSPACE:field3}`,
			},
			want: []string{"field1", "field2", "field3"},
		},
		{
			name: "Pattern with no named groups",
			args: args{
				grokPattern: `%{WORD}/%{NUMBER}/%{NOTSPACE}`,
			},
			want: nil,
		},
		{
			name: "Complex pattern with multiple named groups",
			args: args{
				grokPattern: `AWSLogs/%{WORD:org}/%{NUMBER:account_id}/CloudTrail/%{NOTSPACE:region}/%{YEAR:year}/%{MONTHNUM:month}/%{MONTHDAY:day}/%{WORD:file_name}.%{WORD:ext}`,
			},
			want: []string{"org", "account_id", "region", "year", "month", "day", "file_name", "ext"},
		},
		{
			name: "Pattern with duplicate group names",
			args: args{
				grokPattern: `%{WORD:field1}/%{NUMBER:field1}/%{NOTSPACE:field2}`,
			},
			want: []string{"field1", "field1", "field2"},
		},
		{
			name: "Empty pattern",
			args: args{
				grokPattern: ``,
			},
			want: nil,
		},
		{
			name: "Pattern with invalid format",
			args: args{
				grokPattern: `%{WORD:field1/%{NUMBER:field2}`,
			},
			want: []string{"field2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, ExtractNamedGroupsFromGrok(tt.args.grokPattern), "ExtractNamedGroupsFromGrok(%v)", tt.args.grokPattern)
		})
	}
}

func TestArtifactCollectionState_getGranularityFromMetadata(t *testing.T) {
	type args struct {
		fileLayout string
	}
	tests := []struct {
		name                string
		args                args
		expectedGranularity time.Duration
	}{
		{
			name: "Granularity with year",
			args: args{
				fileLayout: "AWSLogs/%{YEAR:year}/logfile",
			},
			expectedGranularity: time.Hour * 24 * 365, // Year-level granularity
		},
		{
			name: "Granularity with year and month",
			args: args{
				fileLayout: "AWSLogs/%{YEAR:year}/%{MONTHNUM:month}/logfile",
			},
			expectedGranularity: time.Hour * 24 * 30, // Month-level granularity
		},
		{
			name: "Granularity with year, month, and day",
			args: args{
				fileLayout: "AWSLogs/%{YEAR:year}/%{MONTHNUM:month}/%{MONTHDAY:day}/logfile",
			},
			expectedGranularity: time.Hour * 24, // Day-level granularity
		},
		{
			name: "Granularity with year, month, day, and hour",
			args: args{
				fileLayout: "AWSLogs/%{YEAR:year}/%{MONTHNUM:month}/%{MONTHDAY:day}/%{HOUR:hour}/logfile",
			},
			expectedGranularity: time.Hour, // Hour-level granularity
		},
		{
			name: "Granularity with year, month, day, hour, and minute",
			args: args{
				fileLayout: "AWSLogs/%{YEAR:year}/%{MONTHNUM:month}/%{MONTHDAY:day}/%{HOUR:hour}/%{MINUTE:minute}/logfile",
			},
			expectedGranularity: time.Minute, // Minute-level granularity
		},
		{
			name: "Granularity with year, month, day, hour, minute, and second",
			args: args{
				fileLayout: "AWSLogs/%{YEAR:year}/%{MONTHNUM:month}/%{MONTHDAY:day}/%{HOUR:hour}/%{MINUTE:minute}/%{SECOND:second}/logfile",
			},
			expectedGranularity: time.Second, // Second-level granularity
		},
		{
			name: "No time fields in file layout",
			args: args{
				fileLayout: "AWSLogs/logfile",
			},
			expectedGranularity: 0, // No time-related fields
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			granularity := GetGranularityFromFileLayout(&tt.args.fileLayout)
			if granularity != tt.expectedGranularity {
				t.Errorf("getGranularityFromFileLayout() granularity = %v, want %v", granularity, tt.expectedGranularity)
			}
		})
	}
}
