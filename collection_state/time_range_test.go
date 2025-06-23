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
	type fields struct {
		From            time.Time
		To              time.Time
		CollectionOrder CollectionOrder
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
			name: "forward - at end time (inclusive)",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "forward - before end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 6, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "forward - after end time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 8, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "forward - at start time",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - at start time (inclusive)",
			fields: fields{
				From:            time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - after start time",
			fields: fields{
				From:            time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 8, 0, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - before start time",
			fields: fields{
				From:            time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 6, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "reverse - at end time",
			fields: fields{
				From:            time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "forward - within range",
			fields: fields{
				From:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderChronological,
			},
			args: args{
				timestamp: time.Date(2025, 4, 4, 12, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "reverse - within range",
			fields: fields{
				From:            time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
				To:              time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				CollectionOrder: CollectionOrderReverse,
			},
			args: args{
				timestamp: time.Date(2025, 4, 4, 12, 0, 0, 0, time.UTC),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := CollectionTimeRange{
				From:            tt.fields.From,
				To:              tt.fields.To,
				CollectionOrder: tt.fields.CollectionOrder,
			}
			if got := r.onOrInsideUpperBoundary(tt.args.timestamp); got != tt.want {
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

func TestCollectionTimeRange_RangesOverlap(t *testing.T) {
	tests := []struct {
		name   string
		range1 CollectionTimeRange
		range2 CollectionTimeRange
		want   bool
	}{
		{
			name: "no_overlap",
			range1: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-02 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			range2: CollectionTimeRange{
				From:            timeString("2025-01-03 00:00:00"),
				To:              timeString("2025-01-04 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			want: false,
		},
		{
			name: "overlap",
			range1: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-03 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			range2: CollectionTimeRange{
				From:            timeString("2025-01-02 00:00:00"),
				To:              timeString("2025-01-04 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			want: true,
		},
		{
			name: "adjacent_touching",
			range1: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-02 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			range2: CollectionTimeRange{
				From:            timeString("2025-01-02 00:00:00"),
				To:              timeString("2025-01-03 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			want: true,
		},
		{
			name: "one_contains_other",
			range1: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-05 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			range2: CollectionTimeRange{
				From:            timeString("2025-01-02 00:00:00"),
				To:              timeString("2025-01-03 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.range1.RangesOverlap(&tt.range2); got != tt.want {
				t.Errorf("RangesOverlap() = %v, want %v", got, tt.want)
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
			name: "not_subsumed",
			range1: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-03 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			range2: CollectionTimeRange{
				From:            timeString("2025-01-02 00:00:00"),
				To:              timeString("2025-01-04 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			want: false,
		},
		{
			name: "subsumed",
			range1: CollectionTimeRange{
				From:            timeString("2025-01-02 00:00:00"),
				To:              timeString("2025-01-03 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			range2: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-04 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			want: true,
		},
		{
			name: "exact_match",
			range1: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-02 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			range2: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-02 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			want: true,
		},
		{
			name: "same_start_different_end",
			range1: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-03 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			range2: CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-02 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.range1.IsRangeSubsumed(&tt.range2); got != tt.want {
				t.Errorf("IsRangeSubsumed() = %v, want %v", got, tt.want)
			}
		})
	}
}
