package collection_state

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestTimeRangeCollectionState_GetToTime(t *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeObjectState
		Granularity time.Duration
		Order       CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{
			name: "empty_ranges",
			fields: fields{
				TimeRanges:  []*TimeRangeObjectState{},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: time.Time{},
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*TimeRangeObjectState{
					buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: timeString("2024-01-02 00:00:00"),
		},
		{
			name: "multiple_ranges",
			fields: fields{
				TimeRanges: []*TimeRangeObjectState{
					buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
					buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
					buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
				},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: timeString("2024-01-06 00:00:00"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.GetToTime(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("GetToTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeCollectionState_GetFromTime(t *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeObjectState
		Granularity time.Duration
		Order       CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{
			name: "empty_ranges",
			fields: fields{
				TimeRanges:  []*TimeRangeObjectState{},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: time.Time{},
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*TimeRangeObjectState{
					{
						TimeRange: CollectionTimeRange{
							From:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
							To:              time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
							CollectionOrder: CollectionOrderChronological,
						},
					},
				},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "multiple_ranges",
			fields: fields{

				TimeRanges: []*TimeRangeObjectState{
					{
						TimeRange: CollectionTimeRange{
							From:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
							To:              time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
							CollectionOrder: CollectionOrderChronological,
						},
						EndObjects:  map[string]struct{}{"obj1": {}},
						Granularity: time.Hour,
					},
					{
						TimeRange: CollectionTimeRange{
							From:            time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
							To:              time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
							CollectionOrder: CollectionOrderChronological,
						},
						EndObjects:  map[string]struct{}{"obj2": {}},
						Granularity: time.Hour,
					},
				},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.GetFromTime(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("GetFromTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeCollectionState_IsEmpty(t *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeObjectState
		Granularity time.Duration
		Order       CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "empty_ranges",
			fields: fields{
				TimeRanges:  []*TimeRangeObjectState{},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: true,
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*TimeRangeObjectState{
					{
						TimeRange: CollectionTimeRange{
							From:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
							To:              time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
							CollectionOrder: CollectionOrderChronological,
						},
						EndObjects:  map[string]struct{}{"obj1": {}},
						Granularity: time.Hour,
					},
				},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: false,
		},
		{
			name: "multiple_ranges",
			fields: fields{
				TimeRanges: []*TimeRangeObjectState{
					{
						TimeRange: CollectionTimeRange{
							From:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
							To:              time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
							CollectionOrder: CollectionOrderChronological,
						},
						EndObjects:  map[string]struct{}{"obj1": {}},
						Granularity: time.Hour,
					},
					{
						TimeRange: CollectionTimeRange{
							From:            time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
							To:              time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
							CollectionOrder: CollectionOrderChronological,
						},
						EndObjects:  map[string]struct{}{"obj2": {}},
						Granularity: time.Hour,
					},
				},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.IsEmpty(); got != tt.want {
				t1.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeCollectionState_compact(t *testing.T) {
	tests := []struct {
		name           string
		state          *TimeRangeCollectionState
		expectedRanges []*TimeRangeObjectState
	}{
		{
			name: "single_range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological),
			),
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological),
			},
		},
		{
			name: "overlapping_ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			},
		},
		{
			name: "non_overlapping_ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			},
		},
		{
			name: "multiple_overlapping_ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-13 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "empty_ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
			),
			expectedRanges: []*TimeRangeObjectState{},
		},
		{
			name: "adjacent_ranges_with_gap",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "ranges_with_different_granularity",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Minute,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Second, CollectionOrderChronological, "obj3"),
			),
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Second, CollectionOrderChronological, "obj3"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.compact()
			expectedState := buildTimeRangeCollectionState(tt.state.Order, tt.state.Granularity, tt.expectedRanges...)
			if equal, msg := tt.state.Compare(expectedState); !equal {
				t.Error(msg)
			}
		})
	}
}

func TestTimeRangeCollectionState_compactForCollectionPeriod(t *testing.T) {
	tests := []struct {
		name              string
		state             *TimeRangeCollectionState
		currentCollection *CollectionTimeRange
		expectedRanges    []*TimeRangeObjectState
	}{
		{
			name: "single_range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-20 12:00:00"),
				To:              timeString("2025-05-30 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological),
			},
		},
		{
			name: "overlapping_ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-20 12:00:00"),
				To:              timeString("2025-05-30 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			},
		},
		{
			name: "non_overlapping_ranges_outside_collection_period",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-20 12:00:00"),
				To:              timeString("2025-05-30 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			},
		},
		{
			name: "non_overlapping_ranges_within_collection_period",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-04 12:00:00"),
				To:              timeString("2025-05-21 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			},
		},
		{
			name: "multiple_overlapping_ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-13 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-20 12:00:00"),
				To:              timeString("2025-05-30 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "empty_ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-03 12:00:00"),
				To:              timeString("2025-05-10 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{},
		},
		{
			name: "collection_overlaps_multiple_non_contiguous_ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-04 12:00:00"),
				To:              timeString("2025-05-21 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_start_of_first_and_end_of_last_range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-01 12:00:00"),
				To:              timeString("2025-05-25 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_minute_granularity",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Minute,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Minute, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Minute, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-04 12:00:00"),
				To:              timeString("2025-05-21 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Minute, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_hour_granularity",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-04 12:00:00"),
				To:              timeString("2025-05-21 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_day_granularity",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				24*time.Hour,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-04 12:00:00"),
				To:              timeString("2025-05-21 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_mixed_granularity",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Minute,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Second, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &CollectionTimeRange{
				From:            timeString("2025-05-04 12:00:00"),
				To:              timeString("2025-05-21 12:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Minute, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "no_current_collection_time_range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			currentCollection: nil,
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.currentCollectionTimeRange = tt.currentCollection
			tt.state.compactForCollectionPeriod()
			expectedState := buildTimeRangeCollectionState(tt.state.Order, tt.state.Granularity, tt.expectedRanges...)
			if equal, msg := tt.state.Compare(expectedState); !equal {
				t.Error(msg)
			}
		})
	}
}

// TestTimeRangeCollectionState_emulate_collection simulates a collection process then verifies the
// resulting collection state
func TestTimeRangeCollectionState_emulate_collection(t *testing.T) {
	tests := []struct {
		name          string
		state         *TimeRangeCollectionState
		from          time.Time
		to            time.Time
		emptyDays     []time.Time
		order         CollectionOrder
		granularity   time.Duration
		expectedState *TimeRangeCollectionState
	}{
		{
			name:        "First Collection - Forward",
			state:       nil,
			from:        timeString("2025-05-03 12:00:00"),
			to:          timeString("2025-05-10 12:00:00"),
			order:       CollectionOrderChronological,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Hour, CollectionOrderChronological),
			),
		},
		{
			name:        "First Collection - Reverse",
			state:       nil,
			from:        timeString("2025-05-03 12:00:00"),
			to:          timeString("2025-05-10 12:00:00"),
			order:       CollectionOrderReverse,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Hour, CollectionOrderReverse),
			),
		},
		{
			name: "One Existing Range  - Forward - Join existing range",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderChronological, "20250426_000000.txt")),
			from:        timeString("2025-04-26 00:00:00"),
			to:          timeString("2025-05-10 12:00:00"),
			order:       CollectionOrderChronological,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-10 12:00:00", time.Hour, CollectionOrderChronological),
			),
		},
		{
			name: "One Existing Range  - Reverse - join existing range",
			state: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderReverse, "20250426_000000.txt")),
			from:        timeString("2025-04-26 00:00:00"),
			to:          timeString("2025-05-10 12:00:00"),
			order:       CollectionOrderReverse,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-10 12:00:00", time.Hour, CollectionOrderReverse),
			),
		},
		{
			name: "One Existing Range  - Forward - separate from existing range",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderChronological, "20250426_000000.txt")),
			from:        timeString("2025-04-30 00:00:00"),
			to:          timeString("2025-05-10 12:00:00"),
			order:       CollectionOrderChronological,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderChronological, "20250426_000000.txt"),
				buildTimeRangeState("2025-04-30 00:00:00", "2025-05-10 12:00:00", time.Hour, CollectionOrderChronological)),
		},
		{
			name: "One Existing Range  - Reverse - separate from existing range",
			state: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderReverse, "20250426_000000.txt")),
			from:        timeString("2025-04-30 00:00:00"),
			to:          timeString("2025-05-10 12:00:00"),
			order:       CollectionOrderReverse,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderReverse, "20250426_000000.txt"),
				buildTimeRangeState("2025-04-30 00:00:00", "2025-05-10 12:00:00", time.Hour, CollectionOrderReverse)),
		},
		{
			name: "One Existing Range  - Forward - overlap start of existing range",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderChronological, "20250426_000000.txt")),
			from:        timeString("2025-04-10 00:00:00"),
			to:          timeString("2025-04-20 12:00:00"),
			order:       CollectionOrderChronological,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderChronological, "20250426_000000.txt")),
		},
		{
			name: "One Existing Range  - Reverse - overlap start of existing range",
			state: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderReverse, "20250426_000000.txt")),
			from:        timeString("2025-04-10 00:00:00"),
			to:          timeString("2025-04-20 12:00:00"),
			order:       CollectionOrderReverse,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderReverse)),
		},
		{
			name: "One Existing Range  - Forward - overlap end of existing range",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderChronological, "20250426_000000.txt")),
			from:        timeString("2025-04-20 00:00:00"),
			to:          timeString("2025-04-30 12:00:00"),
			order:       CollectionOrderChronological,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-30 12:00:00", time.Hour, CollectionOrderChronological)),
		},
		{
			name: "One Existing Range  - Reverse - overlap end of existing range",
			state: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderReverse, "20250426_000000.txt")),
			from:        timeString("2025-04-20 00:00:00"),
			to:          timeString("2025-04-30 12:00:00"),
			order:       CollectionOrderReverse,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-30 12:00:00", time.Hour, CollectionOrderReverse)),
		},
		{
			name: "One Existing Range  - Forward - subsume existing range",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderChronological, "20250426_000000.txt")),
			from:        timeString("2025-04-10 00:00:00"),
			to:          timeString("2025-04-30 12:00:00"),
			order:       CollectionOrderChronological,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour,
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-30 12:00:00", time.Hour, CollectionOrderChronological)),
		},
		{
			name: "One Existing Range  - Reverse - subsume existing range",
			state: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", time.Hour, CollectionOrderReverse, "20250426_000000.txt")),
			from:        timeString("2025-04-10 00:00:00"),
			to:          timeString("2025-04-30 12:00:00"),
			order:       CollectionOrderReverse,
			granularity: time.Hour,
			expectedState: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour,
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-30 12:00:00", time.Hour, CollectionOrderReverse)),
		},
		/*
			"Multiple Ranges - Forward - Collection between ranges (not joined)"
			"Multiple Ranges - Reverse - Collection between ranges (not joined)"
			"Multiple Ranges - Forward - Collection connects two ranges"
			"Multiple Ranges - Reverse - Collection connects two ranges"
			"Multiple Ranges - Forward - Collection extends from after first into second"
			"Multiple Ranges - Reverse - Collection extends from after first into second"
			"Multiple Ranges - Forward - Collection overlaps end of first range"
			"Multiple Ranges - Reverse - Collection overlaps end of first range"
			"Multiple Ranges - Forward - Collection starts between ranges, extends into second"
			"Multiple Ranges - Reverse - Collection starts between ranges, extends into second"
			"Multiple Ranges - Forward - Collection overlaps both ranges"
			"Multiple Ranges - Reverse - Collection overlaps both ranges"
			"Multiple Ranges - Forward - Collection extends first range backwards"
			"Multiple Ranges - Reverse - Collection extends first range backwards"
			"Multiple Ranges - Forward - Collection extends second range forwards"
			"Multiple Ranges - Reverse - Collection extends second range forwards"
			"Multiple Ranges - Forward - Collection completely subsumes both ranges"
			"Multiple Ranges - Reverse - Collection completely subsumes both ranges"
			"Multiple Ranges - Forward - Collection exactly spans both ranges"
			"Multiple Ranges - Reverse - Collection exactly spans both ranges"
			"Multiple Ranges - Forward - Collection contained within first range"
			"Multiple Ranges - Reverse - Collection contained within first range"
			"Multiple Ranges - Forward - Collection contained within second range"
			"Multiple Ranges - Reverse - Collection contained within second range"
			"Multiple Ranges - Forward - Collection extends first range backwards, doesn't reach second"
			"Multiple Ranges - Reverse - Collection extends first range backwards, doesn't reach second"
			"Multiple Ranges - Forward - Collection extends second range forwards, doesn't reach first"
			"Multiple Ranges - Reverse - Collection extends second range forwards, doesn't reach first"
		*/
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize state if not provided
			state := tt.state
			if state == nil {
				state = NewTimeRangeCollectionState().(*TimeRangeCollectionState)
				// TODO need better way of setting order
				state.Order = tt.order
				// set the granularity
				state.SetGranularity(tt.granularity)
			}

			// Always initialize the state with the collection time range
			state.Init(&CollectionTimeRange{From: tt.from, To: tt.to, CollectionOrder: tt.order})

			// Simulate collection process
			fileTime := tt.from
			increment := tt.granularity
			if tt.order == CollectionOrderReverse {
				fileTime = tt.to
				increment = -tt.granularity
			}

			for {
				// Check if we've reached the end of the collection period
				if tt.order == CollectionOrderChronological && fileTime.After(tt.to) {
					break
				}
				if tt.order == CollectionOrderReverse && fileTime.Before(tt.from) {
					break
				}

				// Skip empty days
				isEmptyDay := false
				for _, emptyDay := range tt.emptyDays {
					if fileTime.Year() == emptyDay.Year() && fileTime.Month() == emptyDay.Month() && fileTime.Day() == emptyDay.Day() {
						isEmptyDay = true
						break
					}
				}

				if !isEmptyDay {
					// Only collect files every 15 minutes
					if fileTime.Minute()%15 == 0 {
						fileName := fmt.Sprintf("file_%s", fileTime.Format("2006-01-02_15-04-05"))
						if state.ShouldCollect(fileName, fileTime) {
							state.OnCollected(fileName, fileTime)
						}
					}
				}

				// Move to next time based on order
				fileTime = fileTime.Add(increment)
			}

			// Complete collection
			state.OnCollectionComplete()

			// Compare final state with expected
			if equal, msg := state.Compare(tt.expectedState); !equal {
				t.Errorf("state after collection: %s", msg)
			}
		})
	}
}

func TestTimeRangeCollectionState_ShouldCollect(t *testing.T) {
	tests := []struct {
		name            string
		state           *TimeRangeCollectionState
		objectTimestamp time.Time
		objectId        string
		want            bool
	}{
		{
			name: "Should collect - no state, timestamp within collection time range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
			),
			objectTimestamp: timeString("2025-04-15 12:00:00"),
			objectId:        "2025-04-15_120000.txt",
			want:            true,
		},
		{
			name: "Should NOT collect - no state, timestamp outside collection time range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
			),
			objectTimestamp: timeString("2025-05-01 00:00:00"),
			objectId:        "2025-05-01_000000.txt",
			want:            false,
		},
		{
			name: "Should collect - timestamp within active range but not present in end objects",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
			),
			objectTimestamp: timeString("2025-04-07 12:00:00"),
			objectId:        "2025-04-07_120000.txt",
			want:            true,
		},
		{
			name: "Should NOT collect - timestamp within active range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
			),
			objectTimestamp: timeString("2025-04-01 12:00:00"),
			objectId:        "2025-04-01_120000.txt",
			want:            false,
		},
		{
			name: "Should NOT collect - timestamp within active range but present in end objects",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
			),
			objectTimestamp: timeString("2025-04-07 00:00:00"),
			objectId:        "20250407_000000.txt",
			want:            false,
		},
		{
			name: "Should collect - timestamp within non active (2nd) range but not present in end objects",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: timeString("2025-04-12 12:00:00"),
			objectId:        "2025-04-12_120000.txt",
			want:            true,
		},
		{
			name: "Should NOT collect - timestamp within non active (2nd) range but present in end objects",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: timeString("2025-04-12 00:00:00"),
			objectId:        "20250412_000000.txt",
			want:            false,
		},
		{
			name: "Should NOT collect - timestamp within non active (2nd) range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: timeString("2025-04-10 00:00:00"),
			objectId:        "2025-04-10_000000.txt",
			want:            false,
		},
		{
			name: "Should collect - timestamp within non active (3rd) range but not present in end objects",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: timeString("2025-04-12 12:00:00"),
			objectId:        "2025-04-12_120000.txt",
			want:            true,
		},
		{
			name: "Should NOT collect - timestamp within non active (3rd) range but present in end objects",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: timeString("2025-04-12 00:00:00"),
			objectId:        "20250412_000000.txt",
			want:            false,
		},
		{
			name: "Should NOT collect - timestamp within non active (3rd) range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: timeString("2025-04-10 00:00:00"),
			objectId:        "2025-04-10_000000.txt",
			want:            false,
		},
	}
	collectionTimeRange := &CollectionTimeRange{
		From:            timeString("2025-04-01 00:00:00"),
		To:              timeString("2025-04-30 00:00:00"),
		CollectionOrder: CollectionOrderChronological,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.Init(collectionTimeRange)
			if got := tt.state.ShouldCollect(tt.objectId, tt.objectTimestamp); got != tt.want {
				t.Errorf("ShouldCollect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeCollectionState_updateActiveRange(t *testing.T) {
	tests := []struct {
		name          string
		state         *TimeRangeCollectionState
		timestamp     time.Time
		expectedState *TimeRangeCollectionState
	}{
		{
			name: "No existing ranges - check initial active range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
			),
			expectedState: setActiveRange(
				buildTimeRangeCollectionState(
					CollectionOrderChronological,
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 00:00:00", time.Hour*24, CollectionOrderChronological),
				),
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "Existing range - extend active range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: timeString("2025-04-08 00:00:00"),
			expectedState: setActiveRange(
				buildTimeRangeCollectionState(
					CollectionOrderChronological,
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
				),
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological)),
		},
		{
			name: "Multiple ranges, timestamp after all ranges - no action (active range would be extended in OnCollected",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: timeString("2025-04-09 00:00:00"),
			expectedState: setActiveRange(
				buildTimeRangeCollectionState(
					CollectionOrderChronological,
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
					buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
					buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
				),
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "Multiple ranges, timestamp in 2nd range ranges - update active range and set original active range end time",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: timeString("2025-04-03 00:00:00"),
			expectedState: setActiveRange(
				buildTimeRangeCollectionState(
					CollectionOrderChronological,
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-03 00:00:00", time.Hour*24, CollectionOrderChronological),
					buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
					buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
				),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
	}

	collectionTimeRange := &CollectionTimeRange{
		From:            timeString("2025-04-01 00:00:00"),
		To:              timeString("2025-04-30 00:00:00"),
		CollectionOrder: CollectionOrderChronological,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.Init(collectionTimeRange)
			// if there is a timestamp, update the active range - this will either set the active range  to an existing range or
			// create a new range with the timestamp as the start time
			if !tt.timestamp.IsZero() {
				tt.state.updateActiveRange(tt.timestamp)
			}

			if equal, msg := tt.state.Compare(tt.expectedState); !equal {
				t.Error(msg)
			}
		})
	}
}

func TestTimeRangeCollectionState_upperBoundaryTime(t *testing.T) {
	tests := []struct {
		name  string
		state *TimeRangeCollectionState
		want  time.Time
	}{
		{
			name:  "empty_ranges_chronological",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour),
			want:  time.Time{},
		},
		{
			name:  "empty_ranges_reverse",
			state: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour),
			want:  time.Time{},
		},
		{
			name: "single_range_chronological",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "single_range_reverse",
			state: buildTimeRangeCollectionState(
				CollectionOrderReverse,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderReverse),
			),
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "multiple_ranges_chronological",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			want: time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "multiple_ranges_reverse",
			state: buildTimeRangeCollectionState(
				CollectionOrderReverse,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderReverse),
			),
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.state.upperBoundaryTime(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("upperBoundaryTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeCollectionState_lowerBoundaryTime(t1 *testing.T) {
	tests := []struct {
		name  string
		state *TimeRangeCollectionState
		want  time.Time
	}{
		{
			name:  "empty_ranges_chronological",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour),
			want:  time.Time{},
		},
		{
			name:  "empty_ranges_reverse",
			state: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour),
			want:  time.Time{},
		},
		{
			name: "single_range_chronological",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			want: timeString("2024-01-01 00:00:00"),
		},
		{
			name: "single_range_reverse",
			state: buildTimeRangeCollectionState(
				CollectionOrderReverse,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderReverse),
			),
			want: timeString("2024-01-02 00:00:00"),
		},
		{
			name: "multiple_ranges_chronological",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			want: timeString("2024-01-01 00:00:00"),
		},
		{
			name: "multiple_ranges_reverse",
			state: buildTimeRangeCollectionState(
				CollectionOrderReverse,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderReverse),
			),
			want: timeString("2024-01-06 00:00:00"),
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.state.lowerBoundaryTime(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("lowerBoundaryTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeCollectionState_rangeForTime(t1 *testing.T) {

	tests := []struct {
		name      string
		state     *TimeRangeCollectionState
		timestamp time.Time
		want      *TimeRangeObjectState
	}{
		{
			name:      "empty state",
			state:     buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour),
			timestamp: timeString("2024-01-01 00:00:00"),
			want:      nil,
		},
		{
			name: "single range - within range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: timeString("2024-01-01 12:00:00"),
			want:      buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "single range - at start time",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: timeString("2024-01-01 00:00:00"),
			want:      buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "single range - at end time",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: timeString("2024-01-02 00:00:00"),
			want:      buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "single range - outside range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: timeString("2024-01-03 00:00:00"),
			want:      nil,
		},
		{
			name: "multiple ranges - within first range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: timeString("2024-01-01 12:00:00"),
			want:      buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "multiple ranges - within middle range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: timeString("2024-01-03 12:00:00"),
			want:      buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "multiple ranges - within last range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: timeString("2024-01-05 12:00:00"),
			want:      buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "multiple ranges - between ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: timeString("2024-01-02 12:00:00"),
			want:      nil,
		},
		{
			name: "multiple ranges - reverse order",
			state: buildTimeRangeCollectionState(
				CollectionOrderReverse,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderReverse),
			),
			timestamp: timeString("2024-01-03 12:00:00"),
			want:      buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderReverse),
		},
		{
			name: "multiple ranges - with end objects",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			),
			timestamp: timeString("2024-01-03 12:00:00"),
			want:      buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological, "obj2"),
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.state.rangeForTime(tt.timestamp); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("rangeForTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeCollectionState_addRange(t1 *testing.T) {
	tests := []struct {
		name          string
		state         *TimeRangeCollectionState
		timestamp     time.Time
		expectedState *TimeRangeCollectionState
	}{
		{
			name: "empty state - add first range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
			),
			timestamp: timeString("2025-04-01 00:00:00"),
			expectedState: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "single range - add new range after",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: timeString("2025-04-08 00:00:00"),
			expectedState: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-08 00:00:00", "2025-04-08 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "multiple ranges - add new range after existing ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: timeString("2025-04-05 00:00:00"),
			expectedState: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-05 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "multiple ranges - add new range between existing ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-04 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: timeString("2025-04-03 00:00:00"),
			expectedState: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-03 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-04 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "multiple ranges - add new range before existing ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-02 00:00:00", "2025-04-03 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-04 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: timeString("2025-04-01 00:00:00"),
			expectedState: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-02 00:00:00", "2025-04-03 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-04 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "different granularity - add new range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 01:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2025-04-01 02:00:00", "2025-04-01 03:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: timeString("2025-04-01 01:30:00"),
			expectedState: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 01:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2025-04-01 01:30:00", "2025-04-01 01:30:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2025-04-01 02:00:00", "2025-04-01 03:00:00", time.Hour, CollectionOrderChronological),
			),
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t *testing.T) {
			tt.state.addRange(tt.timestamp)
			if equal, msg := tt.state.Compare(tt.expectedState); !equal {
				t.Error(msg)
			}
		})
	}
}

func TestTimeRangeCollectionState_MigrateFromLegacyState(t *testing.T) {
	tests := []struct {
		name        string
		legacy      interface{}
		expected    *TimeRangeCollectionState
		expectError bool
	}{
		{
			name:   "migrate TimeRangeCollectionStateLegacy chronological",
			legacy: buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderChronological, "object1", "object2"),
			expected: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*24,
				buildTimeRangeState("2023-10-01 00:00:00", "2023-11-30 01:00:00", time.Hour*24, CollectionOrderChronological, "object1", "object2"),
			),
		},
		{
			name:   "migrate TimeRangeCollectionStateLegacy reverse",
			legacy: buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
			expected: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*24,
				buildTimeRangeState("2023-12-01 01:00:00", "2023-10-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
			),
		},
		{
			name: "migrate ReverseOrderCollectionStateLegacy single range",
			legacy: &ReverseOrderCollectionStateLegacy{
				TimeRanges: []*TimeRangeCollectionStateLegacy{
					buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
				},
			},
			expected: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*24,
				buildTimeRangeState("2023-12-01 01:00:00", "2023-10-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
			),
		},
		{
			name: "migrate ReverseOrderCollectionStateLegacy multiple ranges",
			legacy: &ReverseOrderCollectionStateLegacy{
				TimeRanges: []*TimeRangeCollectionStateLegacy{
					buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-11-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object1"),
					buildTimeRangeCollectionStateLegacy("2023-11-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderReverse, "object2"),
				},
			},
			expected: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*24,
				buildTimeRangeState("2023-11-01 00:00:00", "2023-10-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object1"),
				buildTimeRangeState("2023-12-01 01:00:00", "2023-11-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object2"),
			),
		},
		{
			name:        "invalid JSON",
			legacy:      "invalid json",
			expectError: true,
		},
		{
			name:     "empty object",
			legacy:   map[string]interface{}{},
			expected: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*24),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal the legacy state to JSON
			legacyJSON, err := json.Marshal(tt.legacy)
			if err != nil {
				t.Fatalf("failed to marshal legacy state: %v", err)
			}

			// Create a new state and attempt migration
			state := NewTimeRangeCollectionState().(*TimeRangeCollectionState)
			err = state.MigrateFromLegacyState(legacyJSON)

			// Check error expectations
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Compare the migrated state with expected
			equal, diff := state.Compare(tt.expected)
			if !equal {
				t.Errorf("migrated state does not match expected: %s", diff)
				t.Logf("expected: %+v", tt.expected)
				t.Logf("got: %+v", state)
			}
		})
	}
}

func buildTimeRangeCollectionState(order CollectionOrder, granularity time.Duration, ranges ...*TimeRangeObjectState) *TimeRangeCollectionState {
	return &TimeRangeCollectionState{
		TimeRanges:     ranges,
		Granularity:    granularity,
		ObjectRangeMap: map[string]*TimeRangeObjectState{},
		Order:          order,
	}
}

func setActiveRange(state *TimeRangeCollectionState, activeRange *TimeRangeObjectState) *TimeRangeCollectionState {
	state.activeRange = activeRange
	return state
}

func TestTimeRangeCollectionState_Clear(t *testing.T) {
	tests := []struct {
		name           string
		state          *TimeRangeCollectionState
		clearRange     *CollectionTimeRange
		expectedRanges []*TimeRangeObjectState
	}{
		{
			name: "no_overlapping_ranges",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-01-01 00:00:00", "2025-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2025-01-03 00:00:00", "2025-01-04 00:00:00", time.Hour, CollectionOrderChronological),
			),
			clearRange: &CollectionTimeRange{
				From:            timeString("2025-01-05 00:00:00"),
				To:              timeString("2025-01-06 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-01-01 00:00:00", "2025-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2025-01-03 00:00:00", "2025-01-04 00:00:00", time.Hour, CollectionOrderChronological),
			},
		},
		{
			name: "totally_subsumed_range",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-01-02 00:00:00", "2025-01-03 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			),
			clearRange: &CollectionTimeRange{
				From:            timeString("2025-01-01 00:00:00"),
				To:              timeString("2025-01-04 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{},
		},
		{
			name: "start_overlap_truncate",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-01-01 00:00:00", "2025-01-04 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			),
			clearRange: &CollectionTimeRange{
				From:            timeString("2025-01-02 00:00:00"),
				To:              timeString("2025-01-05 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-01-02 00:00:00", "2025-01-04 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			},
		},
		{
			name: "end_overlap_truncate",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-01-01 00:00:00", "2025-01-04 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			),
			clearRange: &CollectionTimeRange{
				From:            timeString("2025-01-02 00:00:00"),
				To:              timeString("2025-01-03 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-01-02 00:00:00", "2025-01-03 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			},
		},
		{
			name: "both_ends_overlap_truncate",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-01-01 00:00:00", "2025-01-05 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			),
			clearRange: &CollectionTimeRange{
				From:            timeString("2025-01-02 00:00:00"),
				To:              timeString("2025-01-04 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-01-02 00:00:00", "2025-01-04 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
			},
		},
		{
			name: "multiple_ranges_mixed_overlaps",
			state: buildTimeRangeCollectionState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-01-01 00:00:00", "2025-01-02 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-01-02 00:00:00", "2025-01-03 00:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-01-04 00:00:00", "2025-01-05 00:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			),
			clearRange: &CollectionTimeRange{
				From:            timeString("2025-01-02 00:00:00"),
				To:              timeString("2025-01-04 00:00:00"),
				CollectionOrder: CollectionOrderChronological,
			},
			expectedRanges: []*TimeRangeObjectState{
				buildTimeRangeState("2025-01-01 00:00:00", "2025-01-02 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-01-04 00:00:00", "2025-01-05 00:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set the current collection time range
			tt.state.currentCollectionTimeRange = tt.clearRange

			// Call the method
			tt.state.Clear(tt.clearRange)

			if len(tt.state.TimeRanges) != len(tt.expectedRanges) {
				t.Errorf("Expected %d ranges, got %d", len(tt.expectedRanges), len(tt.state.TimeRanges))
				return
			}

			for i, expected := range tt.expectedRanges {
				if i >= len(tt.state.TimeRanges) {
					t.Errorf("Missing range at index %d", i)
					continue
				}

				actual := tt.state.TimeRanges[i]
				if !actual.GetFromTime().Equal(expected.GetFromTime()) {
					t.Errorf("Range %d: expected From time %v, got %v", i, expected.GetFromTime(), actual.GetFromTime())
				}
				if !actual.GetToTime().Equal(expected.GetToTime()) {
					t.Errorf("Range %d: expected To time %v, got %v", i, expected.GetToTime(), actual.GetToTime())
				}
			}
		})
	}
}
