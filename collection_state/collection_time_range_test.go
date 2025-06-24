package collection_state

import (
	"reflect"
	"testing"
	"time"
)

func TestCollectionTimeRange_insideLowerBoundary(t *testing.T) {
	tests := []struct {
		name      string
		timeRange CollectionTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - after start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-02 00:00:00"),
			want:      true,
		},
		{
			name: "forward - at start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name: "forward - within start time granularity",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      true,
		},
		{
			name: "reverse - before end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - at end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - within end time granularity",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-06 12:00:00"),
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.insideLowerBoundary(tt.timestamp); got != tt.want {
				t.Errorf("insideLowerBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_insideUpperBoundary(t *testing.T) {
	tests := []struct {
		name      string
		timeRange CollectionTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - before end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      true,
		},
		{
			name: "forward - at end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name: "forward - within end time granularity",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-06 12:00:00"),
			want:      true,
		},
		{
			name: "reverse - after start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-02 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - at start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - within start time granularity",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.insideUpperBoundary(tt.timestamp); got != tt.want {
				t.Errorf("insideUpperBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_outsideLowerBoundary(t *testing.T) {
	tests := []struct {
		name      string
		timeRange CollectionTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - before start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-03-31 00:00:00"),
			want:      true,
		},
		{
			name: "forward - at start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name: "forward - within start time granularity",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      false,
		},
		{
			name: "reverse - after end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - at end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - within end time granularity",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-06 12:00:00"),
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.outsideLowerBoundary(tt.timestamp); got != tt.want {
				t.Errorf("outsideLowerBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_outsideUpperBoundary(t *testing.T) {
	tests := []struct {
		name      string
		timeRange CollectionTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - after end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name: "forward - at end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      false,
		},
		{
			name: "forward - within end time granularity",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-06 12:00:00"),
			want:      false,
		},
		{
			name: "reverse - before start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-03-31 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - at start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - within start time granularity",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-01 12:00:00"),
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.outsideUpperBoundary(tt.timestamp); got != tt.want {
				t.Errorf("outsideUpperBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_lowerBoundaryTime(t *testing.T) {
	tests := []struct {
		name      string
		timeRange CollectionTimeRange
		want      time.Time
	}{
		{
			name: "forward - returns from time",
			timeRange: CollectionTimeRange{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			want: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "reverse - returns To time",
			timeRange: CollectionTimeRange{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			want: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.lowerBoundaryTime(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("lowerBoundaryTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_upperBoundaryTime(t *testing.T) {
	tests := []struct {
		name      string
		timeRange CollectionTimeRange
		want      time.Time
	}{
		{
			name: "forward - returns To time",
			timeRange: CollectionTimeRange{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			want: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "reverse - returns from time",
			timeRange: CollectionTimeRange{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			want: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.upperBoundaryTime(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("upperBoundaryTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_onOrInsideLowerBoundary(t *testing.T) {
	tests := []struct {
		name      string
		timeRange CollectionTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name: "forward - at start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      true,
		},
		{
			name: "forward - after start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-04-02 00:00:00"),
			want:      true,
		},
		{
			name: "forward - before start time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			timestamp: timeString("2025-03-31 00:00:00"),
			want:      false,
		},
		{
			name: "reverse - at end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - before end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      true,
		},
		{
			name: "reverse - after end time",
			timeRange: CollectionTimeRange{
				From:            timeString("2025-04-01 00:00:00"),
				To:              timeString("2025-04-07 00:00:00"),
				CollectionOrder: CollectionOrderReverse,
			},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.onOrInsideLowerBoundary(tt.timestamp); got != tt.want {
				t.Errorf("onOrInsideLowerBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_onOrInsideUpperBoundary(t *testing.T) {
	tests := []struct {
		name      string
		timeRange CollectionTimeRange
		timestamp time.Time
		want      bool
	}{
		{
			name:      "forward - at end time (inclusive)",
			timeRange: CollectionTimeRange{From: timeString("2025-04-01 00:00:00"), To: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - before end time",
			timeRange: CollectionTimeRange{From: timeString("2025-04-01 00:00:00"), To: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      true,
		},
		{
			name:      "forward - after end time",
			timeRange: CollectionTimeRange{From: timeString("2025-04-01 00:00:00"), To: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      false,
		},
		{
			name:      "forward - at start time",
			timeRange: CollectionTimeRange{From: timeString("2025-04-01 00:00:00"), To: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - at start time (inclusive)",
			timeRange: CollectionTimeRange{From: timeString("2025-04-07 00:00:00"), To: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-07 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - after start time",
			timeRange: CollectionTimeRange{From: timeString("2025-04-07 00:00:00"), To: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-08 00:00:00"),
			want:      true,
		},
		{
			name:      "reverse - before start time",
			timeRange: CollectionTimeRange{From: timeString("2025-04-07 00:00:00"), To: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-06 00:00:00"),
			want:      false,
		},
		{
			name:      "reverse - at end time",
			timeRange: CollectionTimeRange{From: timeString("2025-04-07 00:00:00"), To: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-01 00:00:00"),
			want:      false,
		},
		{
			name:      "forward - within range",
			timeRange: CollectionTimeRange{From: timeString("2025-04-01 00:00:00"), To: timeString("2025-04-07 00:00:00")},
			timestamp: timeString("2025-04-04 12:00:00"),
			want:      true,
		},
		{
			name:      "reverse - within range",
			timeRange: CollectionTimeRange{From: timeString("2025-04-07 00:00:00"), To: timeString("2025-04-01 00:00:00"), CollectionOrder: CollectionOrderReverse},
			timestamp: timeString("2025-04-04 12:00:00"),
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.timeRange.onOrInsideUpperBoundary(tt.timestamp); got != tt.want {
				t.Errorf("onOrInsideUpperBoundary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_setUpperBoundaryTime(t1 *testing.T) {
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
		want   CollectionTimeRange
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
			want: CollectionTimeRange{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
		},
		{
			name: "chronological - no change (new time before current To)",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				newTime: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			},
			want: CollectionTimeRange{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
		},
		{
			name: "chronological - no change (new time equal to current To)",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				newTime: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			},
			want: CollectionTimeRange{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
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
			want: CollectionTimeRange{
				From:            time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
		},
		{
			name: "reverse - no change (new time after current From)",
			fields: fields{
				From:            time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				newTime: time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
			},
			want: CollectionTimeRange{
				From:            time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
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
			want: CollectionTimeRange{
				From:            time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 17, 30, 45, 0, time.UTC),
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
			want: CollectionTimeRange{
				From:            time.Date(2025, 4, 1, 10, 30, 45, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 12, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := CollectionTimeRange{
				From:            tt.fields.From,
				To:              tt.fields.To,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			t.setUpperBoundaryTime(tt.args.newTime)
			if !reflect.DeepEqual(t, tt.want) {
				t1.Errorf("setUpperBoundaryTime() = %v, want %v", t, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_IsRangeSubsumed(t *testing.T) {
	tests := []struct {
		name   string
		range1 CollectionTimeRange
		range2 CollectionTimeRange
		want   bool
	}{
		{
			/*  1234
			    AA--
			    -BB-
			*/
			name:   "not_subsumed",
			range1: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			range2: CollectionTimeRange{From: timeString("2025-01-02 00:00:00"), To: timeString("2025-01-04 00:00:00")},
			want:   false,
		},
		{
			/*  1234
			    --A-
			    BBB-
			*/
			name:   "subsumed",
			range1: CollectionTimeRange{From: timeString("2025-01-02 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			range2: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-04 00:00:00")},
			want:   true,
		},
		{
			/*  1234
			    A-
			    B-
			*/
			name:   "exact_match",
			range1: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-02 00:00:00")},
			range2: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-02 00:00:00")},
			want:   true,
		},
		{
			/*  123
			    AA-
			    B--
			*/
			name:   "same_start_different_end",
			range1: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			range2: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-02 00:00:00")},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.range1.IsRangeSubsumed(tt.range2); got != tt.want {
				t.Errorf("IsRangeSubsumed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectionTimeRange_OverlapsEnd(t1 *testing.T) {
	tests := []struct {
		name  string
		us    CollectionTimeRange
		other CollectionTimeRange
		want  bool
	}{
		{
			/*  12345
			    --AA-
			    BBB--
			*/
			name:  "our start overlaps other end",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-04 00:00:00")},
			want:  true,
		},
		{
			/*  1234567
			    ----AA-
			    BB-----
			*/
			name:  "no overlap - other completely before",
			us:    CollectionTimeRange{From: timeString("2025-01-05 00:00:00"), To: timeString("2025-01-07 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			want:  false,
		},
		{
			/*  1234567
			    AA-----
			    ----BB-
			*/
			name:  "no overlap - other completely after",
			us:    CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-05 00:00:00"), To: timeString("2025-01-07 00:00:00")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "exact boundary match - no overlap",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "other end exactly at our start - no overlap",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "edge case - minimal overlap",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:01"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:02")},
			want:  true,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "edge case - other end just after our start",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:01")},
			want:  true,
		},
		{
			/*  1234567
			    --AA---
			    BBBBB--
			*/
			name:  "other extends beyond our end - no overlap",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-07 00:00:00")},
			want:  false, // This should be false because other.To is not before our To
		},
		{
			/*  12345
			    --AA-
			    --AA-
			*/
			name:  "edge case - other end exactly at our start boundary",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    B----
			*/
			name:  "edge case - other end just before our start",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-02 23:59:59")},
			want:  false,
		},
		{
			/*  12345
			    --AA-
			    BB---
			*/
			name:  "edge case - other end just after our start",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:01")},
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

func TestCollectionTimeRange_OverlapsStart(t1 *testing.T) {
	tests := []struct {
		name  string
		us    CollectionTimeRange
		other CollectionTimeRange
		want  bool
	}{
		{
			/*  12345
				AAA-
			    --BB
			*/
			name:  "our end overlaps other start",
			us:    CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-04 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			want:  true,
		},
		{
			/*  1234567
			    AAAA---
			    --BBBB--
			*/
			name:  "other starts overlaps our end",
			us:    CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-07 00:00:00")},
			want:  true,
		},
		{
			/*  1234567
			    ----AA-
			    BB-----
			*/
			name:  "no overlap - other completely before",
			us:    CollectionTimeRange{From: timeString("2025-01-05 00:00:00"), To: timeString("2025-01-07 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			want:  false,
		},
		{
			/*  1234567
			    AA-----
			    ----BB-
			*/
			name:  "no overlap - other completely after",
			us:    CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-05 00:00:00"), To: timeString("2025-01-07 00:00:00")},
			want:  false,
		},
		{
			/*  1234567
			    AA-----
			    --BB---
			*/
			name:  "other start exactly at our end - no overlap",
			us:    CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			want:  false,
		},
		{
			/*  1234567
			    AA-----
			    --BB---
			*/
			name:  "1s overlap at end",
			us:    CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:02")},
			other: CollectionTimeRange{From: timeString("2025-01-03 00:00:01"), To: timeString("2025-01-05 00:00:00")},
			want:  true,
		},
		{
			/*  1234567
			    --AA---
			    BBBBBB-
			*/
			name:  "edge case - other subsumes us",
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-07 00:00:00")},
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
			us:    CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-07 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
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
			us:    CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-03 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-05 00:00:00")},
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
			us:    CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-04 00:00:00")},
			want:  false,
		},
		{
			/*
					1234567
				    AAAA---
				    --BB---
			*/
			name:  "other overlaps out end but To times the same",
			us:    CollectionTimeRange{From: timeString("2025-01-01 00:00:00"), To: timeString("2025-01-05 00:00:00")},
			other: CollectionTimeRange{From: timeString("2025-01-03 00:00:00"), To: timeString("2025-01-04 00:00:00")},
			want:  true,
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
