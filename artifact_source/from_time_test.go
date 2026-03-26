package artifact_source

import (
	"testing"
	"time"
)

func Test_dirSatisfiesFromTime(t *testing.T) {
	type args struct {
		fromTime time.Time
		metadata map[string]string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "No year, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{},
			},

			want: true,
		},
		{
			name: "Year only, prev year, should be excluded",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year": "2024",
				},
			},
			want: false,
		},
		{
			name: "Year only, same year, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year": "2025",
				},
			},
			want: true,
		},
		{
			name: "Year only, next year, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year": "2026",
				},
			},
			want: true,
		},
		{
			name: "Year only, invalid year, should be excluded",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year": "invalid",
				},
			},
			want: false,
		},

		{
			name: "Year and month, prev year, should be excluded",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2024",
					"month": "6",
				},
			},
			want: false,
		},
		{
			name: "Year and month, same year, prev month, should be excluded",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2025",
					"month": "5",
				},
			},
			want: false,
		},
		{

			name: "Year and month, same year, same month, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 9, 0, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2025",
					"month": "06",
				},
			},
			want: true,
		},
		{
			name: "Year and month, same year, next month, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2025",
					"month": "7",
				},
			},
			want: true,
		},
		{
			name: "Year and month, invalid year, should be excluded",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "invalid",
					"month": "6",
				},
			},
			want: false,
		},
		{
			name: "Year and month, invalid month, should be excluded",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2025",
					"month": "invalid",
				},
			},
			want: false,
		},
		{
			name: "Year and month, next year, prev month, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "5",
				},
			},
			want: true,
		},
		{
			name: "Year and month, next year, same month, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "6",
				},
			},
			want: true,
		},
		{
			name: "Year and month, next year, next month, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "7",
				},
			},
			want: true,
		},
		{
			name: "Year month and day, prev year, should be excluded",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2024",
					"month": "6",
					"day":   "6",
				},
			},
			want: false,
		},
		{
			name: "Year month and day, same year, prev month, should be excluded",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2025",
					"month": "5",
					"day":   "6",
				},
			},
			want: false,
		},
		{
			name: "Year month and day, same year, same month, prev day, should be excluded",
			args: args{

				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2025",
					"month": "6",
					"day":   "5",
				},
			},
			want: false,
		},
		{
			name: "Year month and day, same year, same month, same day, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2025",
					"month": "6",
					"day":   "6",
				},
			},
			want: true,
		},
		{
			name: "Year month and day, same year, same month, next day, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2025",
					"month": "6",
					"day":   "7",
				},
			},
			want: true,
		},
		{
			name: "Year month and day, next year, prev month, prev day, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "5",
					"day":   "5",
				},
			},
			want: true,
		},
		{
			name: "Year month and day, next year, same month, prev day, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "6",
					"day":   "5",
				},
			},
			want: true,
		},
		{
			name: "Year month and day, next year, same month, same day, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "6",
					"day":   "5",
				},
			},
			want: true,
		},
		{
			name: "Year month and day, next year, same month, same day, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "6",
					"day":   "6",
				},
			},
			want: true,
		},
		{
			name: "Year month and day, next year, next month, prev day, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "7",
					"day":   "5",
				},
			},
			want: true,
		},

		{
			name: "Year month and day, next year, next month, same day, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "7",
					"day":   "6",
				},
			},
			want: true,
		},
		{
			name: "Year month and day, next year, next month, next day, should be included",
			args: args{
				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "7",
					"day":   "7",
				},
			},
			want: true,
		},
		// Test invalid day
		{
			name: "Year month and day, invalid day, should be excluded",
			args: args{

				fromTime: time.Date(2025, 6, 6, 6, 0, 0, 0, time.UTC),
				metadata: map[string]string{
					"year":  "2026",
					"month": "7",
					"day":   "invalid",
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			got := dirSatisfiesFromTime(tt.args.fromTime, tt.args.metadata)
			if got != tt.want {
				t.Errorf("dirSatisfiesFromTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
