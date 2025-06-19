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
		order     CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// Forward order tests (chronological)
		{
			name: "forward - before range",
			fields: fields{
				from: timeString("2024-01-02 00:00:00"),
				to:   timeString("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-01 00:00:00"),
				order:     CollectionOrderChronological,
			},
			want: false,
		},
		{
			name: "forward - at start (inclusive)",
			fields: fields{
				from: timeString("2024-01-02 00:00:00"),
				to:   timeString("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-02 00:00:00"),
				order:     CollectionOrderChronological,
			},
			want: true,
		},
		{
			name: "forward - within range",
			fields: fields{
				from: timeString("2024-01-02 00:00:00"),
				to:   timeString("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-02 12:00:00"),
				order:     CollectionOrderChronological,
			},
			want: true,
		},
		{
			name: "forward - at end (exclusive)",
			fields: fields{
				from: timeString("2024-01-02 00:00:00"),
				to:   timeString("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-04 00:00:00"),
				order:     CollectionOrderChronological,
			},
			want: false, // End time is exclusive
		},
		{
			name: "forward - after range",
			fields: fields{
				from: timeString("2024-01-02 00:00:00"),
				to:   timeString("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-05 00:00:00"),
				order:     CollectionOrderChronological,
			},
			want: false,
		},
		// Reverse order tests
		{
			name: "reverse - before range",
			fields: fields{
				from: timeString("2024-01-04 00:00:00"), // In reverse, 'from' is the later time
				to:   timeString("2024-01-02 00:00:00"), // In reverse, 'to' is the earlier time
			},
			args: args{
				timestamp: timeString("2024-01-01 00:00:00"),
				order:     CollectionOrderReverse,
			},
			want: false,
		},
		{
			name: "reverse - at start (exclusive)",
			fields: fields{
				from: timeString("2024-01-04 00:00:00"),
				to:   timeString("2024-01-02 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-04 00:00:00"),
				order:     CollectionOrderReverse,
			},
			want: false, // Start time is exclusive in reverse order
		},
		{
			name: "reverse - within range",
			fields: fields{
				from: timeString("2024-01-04 00:00:00"),
				to:   timeString("2024-01-02 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-03 00:00:00"),
				order:     CollectionOrderReverse,
			},
			want: true,
		},
		{
			name: "reverse - at end (inclusive)",
			fields: fields{
				from: timeString("2024-01-04 00:00:00"),
				to:   timeString("2024-01-02 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-02 00:00:00"),
				order:     CollectionOrderReverse,
			},
			want: true, // End time is inclusive in reverse order
		},
		{
			name: "reverse - just before end (inclusive)",
			fields: fields{
				from: timeString("2024-01-04 00:00:00"),
				to:   timeString("2024-01-02 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-02 00:00:01"),
				order:     CollectionOrderReverse,
			},
			want: true,
		},
		{
			name: "reverse - just after start (inclusive)",
			fields: fields{
				from: timeString("2024-01-04 00:00:00"),
				to:   timeString("2024-01-02 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-03 23:59:59"),
				order:     CollectionOrderReverse,
			},
			want: true,
		},
		{
			name: "reverse - after range",
			fields: fields{
				from: timeString("2024-01-04 00:00:00"),
				to:   timeString("2024-01-02 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-05 00:00:00"),
				order:     CollectionOrderReverse,
			},
			want: false,
		},
		// Edge cases
		{
			name: "zero time range",
			fields: fields{
				from: time.Time{},
				to:   time.Time{},
			},
			args: args{
				timestamp: timeString("2024-01-02 00:00:00"),
				order:     CollectionOrderChronological,
			},
			want: false,
		},
		{
			name: "zero time range reverse",
			fields: fields{
				from: time.Time{},
				to:   time.Time{},
			},
			args: args{
				timestamp: timeString("2024-01-02 00:00:00"),
				order:     CollectionOrderReverse,
			},
			want: false,
		},
		// Boundary edge cases
		{
			name: "forward - just before end (inclusive)",
			fields: fields{
				from: timeString("2024-01-02 00:00:00"),
				to:   timeString("2024-01-04 00:00:00"),
			},
			args: args{
				timestamp: timeString("2024-01-03 23:59:59"),
				order:     CollectionOrderChronological,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := TimeRange{
				From: tt.fields.from,
				To:   tt.fields.to,
			}
			if got := r.Contains(tt.args.timestamp, tt.args.order); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}
