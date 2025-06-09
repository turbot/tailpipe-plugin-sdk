package collection_state

import (
	"testing"
	"time"
)

func Test_timeRange_Contains(t *testing.T) {
	type fields struct {
		from time.Time
		to   time.Time
	}
	type args struct {
		timestamp time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "timestamp before range",
			fields: fields{
				from: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				to:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			},
			args: args{
				timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "timestamp at range start",
			fields: fields{
				from: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				to:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			},
			args: args{
				timestamp: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "timestamp in middle of range",
			fields: fields{
				from: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				to:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			},
			args: args{
				timestamp: time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "timestamp at range end - FALSE",
			fields: fields{
				from: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				to:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			},
			args: args{
				timestamp: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "timestamp after range",
			fields: fields{
				from: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				to:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			},
			args: args{
				timestamp: time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "zero time range",
			fields: fields{
				from: time.Time{},
				to:   time.Time{},
			},
			args: args{
				timestamp: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "timestamp with different timezone",
			fields: fields{
				from: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				to:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			},
			args: args{
				timestamp: time.Date(2024, 1, 2, 12, 0, 0, 0, time.FixedZone("EST", -5*3600)),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := timeRange{
				from: tt.fields.from,
				to:   tt.fields.to,
			}
			if got := r.Contains(tt.args.timestamp); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}
