package helpers

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
			want: []string{},
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
			want: []string{},
		},
		{
			name: "Pattern with invalid format",
			args: args{
				grokPattern: `%{WORD:field1/%{NUMBER:field2}`,
			},
			want: []string{"field1", "field2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, ExtractNamedGroupsFromGrok(tt.args.grokPattern), "ExtractNamedGroupsFromGrok(%v)", tt.args.grokPattern)
		})
	}
}
