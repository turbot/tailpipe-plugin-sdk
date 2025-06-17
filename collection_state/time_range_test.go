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
			name: "before range",
			fields: fields{
				from: parseTime("2024-01-02 00:00:00"),
				to:   parseTime("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: parseTime("2024-01-01 00:00:00"),
			},
			want: false,
		},
		{
			name: "at start",
			fields: fields{
				from: parseTime("2024-01-02 00:00:00"),
				to:   parseTime("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: parseTime("2024-01-02 00:00:00"),
			},
			want: true,
		},
		{
			name: "within range",
			fields: fields{
				from: parseTime("2024-01-02 00:00:00"),
				to:   parseTime("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: parseTime("2024-01-02 12:00:00"),
			},
			want: true,
		},
		{
			name: "at end",
			fields: fields{
				from: parseTime("2024-01-02 00:00:00"),
				to:   parseTime("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: parseTime("2024-01-04 00:00:00"),
			},
			want: false, // Note: end time is exclusive
		},
		{
			name: "after range",
			fields: fields{
				from: parseTime("2024-01-02 00:00:00"),
				to:   parseTime("2024-01-05 00:00:00"),
			},
			args: args{
				timestamp: parseTime("2024-01-05 00:00:00"),
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
				timestamp: parseTime("2024-01-02 00:00:00"),
			},
			want: false,
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
