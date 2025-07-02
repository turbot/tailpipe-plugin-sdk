package collection_state

import (
	"reflect"
	"testing"
	"time"
)

func TestDirectionalTimeRange_afterStart(t *testing.T) {
	tests := []struct {
		name      string
		timeRange DirectionalTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - after start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-02 00:00:00"),
			want:      true,
		},
		{
			name: "forward - at start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name: "forward - within start time granularity",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      true,
		},
		{
			name: "reverse - before end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - at end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - within end time granularity",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-06 12:00:00"),
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.AfterStart(tt.timestamp); got != tt.want {
				t.Errorf("AfterStart() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_beforeEnd(t *testing.T) {
	tests := []struct {
		name      string
		timeRange DirectionalTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - before end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      true,
		},
		{
			name: "forward - at end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name: "forward - within end time granularity",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-06 12:00:00"),
			want:      true,
		},
		{
			name: "reverse - after start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-02 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - at start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - within start time granularity",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.BeforeEnd(tt.timestamp); got != tt.want {
				t.Errorf("BeforeEnd() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_beforeStart(t *testing.T) {
	tests := []struct {
		name      string
		timeRange DirectionalTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - before start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-03-31 00:00:00"),
			want:      true,
		},
		{
			name: "forward - at start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name: "forward - within start time granularity",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      false,
		},
		{
			name: "reverse - after end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - at end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - within end time granularity",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-06 12:00:00"),
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.BeforeStart(tt.timestamp); got != tt.want {
				t.Errorf("BeforeStart() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_afterEnd(t *testing.T) {
	tests := []struct {
		name      string
		timeRange DirectionalTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - after end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name: "forward - at end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name: "forward - within end time granularity",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-06 12:00:00"),
			want:      false,
		},
		{
			name: "reverse - before start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-03-31 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - at start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - within start time granularity",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.AfterEnd(tt.timestamp); got != tt.want {
				t.Errorf("AfterEnd() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_startTime(t *testing.T) {
	tests := []struct {
		name      string
		timeRange DirectionalTimeRange
		want      time.Time
	}{
		{
			name: "forward - returns from time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			want: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "reverse - returns UpperBoundary time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			want: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.StartTime(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StartTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_endTime(t *testing.T) {
	tests := []struct {
		name      string
		timeRange DirectionalTimeRange
		want      time.Time
	}{
		{
			name: "forward - returns UpperBoundary time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			want: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "reverse - returns from time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			want: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.EndTime(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EndTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_onOrBeforeStart(t *testing.T) {
	tests := []struct {
		name      string
		timeRange DirectionalTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - at start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      true,
		},
		{
			name: "forward - after start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-02 00:00:00"),
			want:      true,
		},
		{
			name: "forward - before start time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-03-31 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - at end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - before end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - after end time",
			timeRange: DirectionalTimeRange{
				LowerBoundary:   timeString("2025-04-01 00:00:00"),
				UpperBoundary:   timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.OnOrAfterStart(tt.timestamp); got != tt.want {
				t.Errorf("OnOrAfterStart() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_onOrBeforeEnd(t *testing.T) {
	tests := []struct {
		name      string
		timeRange DirectionalTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name:      "forward - at end time (inclusive)",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-01 00:00:00"), UpperBoundary: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - before end time",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-01 00:00:00"), UpperBoundary: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - after end time",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-01 00:00:00"), UpperBoundary: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      false,
		},
		{
			name:      "forward - at start time",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-01 00:00:00"), UpperBoundary: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - at start time (inclusive)",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-07 00:00:00"), UpperBoundary: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - after start time",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-07 00:00:00"), UpperBoundary: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - before start time",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-07 00:00:00"), UpperBoundary: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      false,
		},
		{
			name:      "reverse - at end time",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-07 00:00:00"), UpperBoundary: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name:      "forward - within range",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-01 00:00:00"), UpperBoundary: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-04 12:00:00"),
			want:      true,
		},
		{
			name:      "reverse - within range",
			timeRange: DirectionalTimeRange{LowerBoundary: timeString("2025-04-07 00:00:00"), UpperBoundary: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-04 12:00:00"),
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.OnOrBeforeEnd(tt.timestamp); got != tt.want {
				t.Errorf("OnOrBeforeEnd() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_extendEndTime(t1 *testing.T) {
	type fields struct {
		From            time.Time
		To              time.Time
		CollectionOrder CollectionOrder
	}
	type args struct {
		newTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   DirectionalTimeRange
	}{
		{
			name: "chronological - extend range",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				newTime: time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
			},
			want: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
		},
		{
			name: "chronological - no change (new time before current UpperBoundary)",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				newTime: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			},
			want: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
		},
		{
			name: "chronological - no change (new time equal to current UpperBoundary)",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				newTime: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			},
			want: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
		},
		{
			name: "reverse - extend range",
			fields: fields{
				From:            time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				newTime: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			},
			want: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
		},
		{
			name: "reverse - no change (new time after current LowerBoundary)",
			fields: fields{
				From:            time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				newTime: time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
			},
			want: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
		},
		{
			name: "chronological - extend with granular time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 15, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				newTime: time.Date(2025, 4, 1, 17, 30, 45, 0, time.UTC),
			},
			want: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 1, 17, 30, 45, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
		},
		{
			name: "reverse - extend with granular time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 15, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				newTime: time.Date(2025, 4, 1, 10, 30, 45, 0, time.UTC),
			},
			want: DirectionalTimeRange{
				LowerBoundary:   time.Date(2025, 4, 1, 10, 30, 45, 0, time.UTC),
				UpperBoundary:   time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := DirectionalTimeRange{
				LowerBoundary:   tt.fields.From,
				UpperBoundary:   tt.fields.To,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			t.extendEndTime(tt.args.newTime)
			if !reflect.DeepEqual(t, tt.want) {
				t1.Errorf("extendEndTime() = %v, want %v", t, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_IsRangeSubsumed(t *testing.T) {
	tests := []struct {
		name   string
		range1 DirectionalTimeRange
		range2 DirectionalTimeRange
		want   bool
	}{
		{
			/*  1234
			    AA--
			    -BB-
			*/
			name:   "not_subsumed",
			range1: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			range2: DirectionalTimeRange{LowerBoundary: timeString("2025-01-02 00:00:00"), UpperBoundary: timeString("2025-01-04 00:00:00")},
			want:   false,
		},
		{
			/*  1234
			    --A-
			    BBB-
			*/
			name:   "subsumed",
			range1: DirectionalTimeRange{LowerBoundary: timeString("2025-01-02 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			range2: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-04 00:00:00")},
			want:   true,
		},
		{
			/*  1234
			    A-
			    B-
			*/
			name:   "exact_match",
			range1: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-02 00:00:00")},
			range2: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-02 00:00:00")},
			want:   true,
		},
		{
			/*  123
			    AA-
			    B--
			*/
			name:   "same_start_different_end",
			range1: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			range2: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-02 00:00:00")},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.range1.IsSubsumedBy(tt.range2); got != tt.want {
				t.Errorf("IsSubsumedBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_OverlapsEnd(t1 *testing.T) {
	tests := []struct {
		name  string
		us    DirectionalTimeRange
		other DirectionalTimeRange
		want  bool
	}{
		{
			/*  12345
			    --AA-
			    BBB--
			*/
			name:  "our start overlaps other end",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-04 00:00:00")},
			want:  true,
		},
		{
			/*  1234567
			    ----AA-
			    BB-----
			*/
			name:  "no overlap - other completely before",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-05 00:00:00"), UpperBoundary: timeString("2025-01-07 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			want:  false,
		},
		{
			/*  1234567
			    AA-----
			    ----BB-
			*/
			name:  "no overlap - other completely after",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-05 00:00:00"), UpperBoundary: timeString("2025-01-07 00:00:00")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "exact boundary match - no overlap",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "other end exactly at our start - no overlap",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "edge case - minimal overlap",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:01"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:02")},
			want:  true,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "edge case - other end just after our start",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:01")},
			want:  true,
		},
		{
			/*  1234567
			    --AA---
			    BBBBB--
			*/
			name:  "other extends beyond our end - no overlap",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-07 00:00:00")},
			want:  false, // This should be false because other.UpperBoundary is not before our UpperBoundary
		},
		{
			/*  12345
			    --AA-
			    --AA-
			*/
			name:  "edge case - other end exactly at our start boundary",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    B----
			*/
			name:  "edge case - other end just before our start",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-02 23:59:59")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "edge case - other end just after our start",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:01")},
			want:  true,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.us.OverlapsEnd(tt.other); got != tt.want {
				t1.Errorf("OverlapsEnd() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_OverlapsStart(t1 *testing.T) {
	tests := []struct {
		name  string
		us    DirectionalTimeRange
		other DirectionalTimeRange
		want  bool
	}{
		{
			/*  12345
				AAA-
			    --BB
			*/
			name:  "our end overlaps other start",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-04 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			want:  true,
		},
		{
			/*  1234567
			    AAAA---
			    --BBBB--
			*/
			name:  "other starts overlaps our end",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-07 00:00:00")},
			want:  true,
		},
		{
			/*  1234567
			    ----AA-
			    BB-----
			*/
			name:  "no overlap - other completely before",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-05 00:00:00"), UpperBoundary: timeString("2025-01-07 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			want:  false,
		},
		{
			/*  1234567
			    AA-----
			    ----BB-
			*/
			name:  "no overlap - other completely after",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-05 00:00:00"), UpperBoundary: timeString("2025-01-07 00:00:00")},
			want:  false,
		},
		{
			/*  1234567
			    AA-----
			    --BB---
			*/
			name:  "other start exactly at our end - no overlap",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			want:  false,
		},
		{
			/*  1234567
			    AA-----
			    --BB---
			*/
			name:  "1s overlap at end",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:02")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:01"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			want:  true,
		},
		{
			/*  1234567
			    --AA---
			    BBBBBB-
			*/
			name:  "edge case - other subsumes us",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-07 00:00:00")},
			// return false if either range subsumes the other
			want: false,
		},
		{
			/*
					1234567
				    AAAAAA-
				    --BB---
			*/
			name:  "edge case - we subsume other",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-07 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			// return false if either range subsumes the other
			want: false,
		},
		{
			/*
					1234567
				    AA-----
				    BBBB---
			*/
			name:  "edge case - other starts exactly at our start and subsumes us",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-03 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			// return false if either range subsumes the other
			want: false,
		},
		{
			/*
					1234567
				    --AA---
				    BBB----
			*/
			name:  "other overlaps our start",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-04 00:00:00")},
			want:  false,
		},
		{
			/*
					1234567
				    AAAA---
				    --BB---
			*/
			name:  "other overlaps out end but UpperBoundary times the same",
			us:    DirectionalTimeRange{LowerBoundary: timeString("2025-01-01 00:00:00"), UpperBoundary: timeString("2025-01-05 00:00:00")},
			other: DirectionalTimeRange{LowerBoundary: timeString("2025-01-03 00:00:00"), UpperBoundary: timeString("2025-01-04 00:00:00")},
			want:  false,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.us.OverlapsStart(tt.other); got != tt.want {
				t1.Errorf("OverlapsStart() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectionalTimeRange_Validate(t1 *testing.T) {
	type fields struct {
		LowerBoundary   time.Time
		UpperBoundary   time.Time
		CollectionOrder CollectionOrder
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid_chronological_order",
			fields: fields{
				LowerBoundary:   timeString("2024-01-01 00:00:00"),
				UpperBoundary:   timeString("2024-01-02 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			wantErr: false,
		},
		{
			name: "valid_reverse_order",
			fields: fields{
				LowerBoundary:   timeString("2024-01-01 00:00:00"),
				UpperBoundary:   timeString("2024-01-02 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			wantErr: false,
		},
		{
			name: "valid_same_time_boundaries",
			fields: fields{
				LowerBoundary:   timeString("2024-01-01 00:00:00"),
				UpperBoundary:   timeString("2024-01-01 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			wantErr: false,
		},
		{
			name: "valid_with_granular_times",
			fields: fields{
				LowerBoundary:   timeString("2024-01-01 12:30:45"),
				UpperBoundary:   timeString("2024-01-02 15:45:30"),
				CollectionOrder: CollectionOrderChronological,
			},
			wantErr: false,
		},
		{
			name: "zero_lower_boundary",
			fields: fields{
				LowerBoundary:   time.Time{},
				UpperBoundary:   timeString("2024-01-02 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			wantErr: true,
		},
		{
			name: "zero_upper_boundary",
			fields: fields{
				LowerBoundary:   timeString("2024-01-01 00:00:00"),
				UpperBoundary:   time.Time{},
				CollectionOrder: CollectionOrderChronological,
			},
			wantErr: true,
		},
		{
			name: "both_boundaries_zero",
			fields: fields{
				LowerBoundary:   time.Time{},
				UpperBoundary:   time.Time{},
				CollectionOrder: CollectionOrderChronological,
			},
			wantErr: true,
		},
		{
			name: "lower_boundary_after_upper_boundary_chronological",
			fields: fields{
				LowerBoundary:   timeString("2024-01-02 00:00:00"),
				UpperBoundary:   timeString("2024-01-01 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			wantErr: true,
		},
		{
			name: "lower_boundary_after_upper_boundary_reverse",
			fields: fields{
				LowerBoundary:   timeString("2024-01-02 00:00:00"),
				UpperBoundary:   timeString("2024-01-01 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			wantErr: true,
		},
		{
			name: "invalid_collection_order_unknown",
			fields: fields{
				LowerBoundary:   timeString("2024-01-01 00:00:00"),
				UpperBoundary:   timeString("2024-01-02 00:00:00"),
				CollectionOrder: CollectionOrder(999),
			},
			wantErr: true,
		},
		{
			name: "invalid_collection_order_negative",
			fields: fields{
				LowerBoundary:   timeString("2024-01-01 00:00:00"),
				UpperBoundary:   timeString("2024-01-02 00:00:00"),
				CollectionOrder: CollectionOrder(-1),
			},
			wantErr: true,
		},
		{
			name: "valid_large_time_range",
			fields: fields{
				LowerBoundary:   timeString("2020-01-01 00:00:00"),
				UpperBoundary:   timeString("2030-12-31 23:59:59"),
				CollectionOrder: CollectionOrderChronological,
			},
			wantErr: false,
		},
		{
			name: "valid_small_time_range",
			fields: fields{
				LowerBoundary:   timeString("2024-01-01 00:00:00"),
				UpperBoundary:   timeString("2024-01-01 00:00:01"),
				CollectionOrder: CollectionOrderChronological,
			},
			wantErr: false,
		},
		{
			name: "valid_reverse_with_large_range",
			fields: fields{
				LowerBoundary:   timeString("2030-12-31 23:59:59"),
				UpperBoundary:   timeString("2020-01-01 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			wantErr: true, // This should still fail because LowerBoundary > UpperBoundary
		},
		{
			name: "valid_reverse_with_proper_order",
			fields: fields{
				LowerBoundary:   timeString("2024-01-02 00:00:00"),
				UpperBoundary:   timeString("2024-01-01 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			wantErr: true, // This should still fail because LowerBoundary > UpperBoundary
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &DirectionalTimeRange{
				LowerBoundary:   tt.fields.LowerBoundary,
				UpperBoundary:   tt.fields.UpperBoundary,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			if err := t.Validate(); (err != nil) != tt.wantErr {
				t1.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
