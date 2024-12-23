package collection_state

import (
	"testing"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_source_config"
)

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
			state := &ArtifactCollectionState[*artifact_source_config.ArtifactSourceConfigBase]{}
			state.getGranularityFromMetadata(tt.args.fileLayout)
			if state.granularity != tt.expectedGranularity {
				t.Errorf("getGranularityFromMetadata() granularity = %v, want %v", state.granularity, tt.expectedGranularity)
			}
		})
	}
}
