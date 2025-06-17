package collection_state

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestTimeRangeSliceCollectionState_GetToTime(t *testing.T) {
	type fields struct {
		TimeRanges  []*timeRangeCollectionState
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
				TimeRanges:  []*timeRangeCollectionState{},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: time.Time{},
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: parseTime("2024-01-02 00:00:00"),
		},
		{
			name: "multiple_ranges",
			fields: fields{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
					buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
					buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
				},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: parseTime("2024-01-06 00:00:00"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
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

func TestTimeRangeSliceCollectionState_GetFromTime(t *testing.T) {
	type fields struct {
		TimeRanges  []*timeRangeCollectionState
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
				TimeRanges:  []*timeRangeCollectionState{},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: time.Time{},
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*timeRangeCollectionState{
					{
						From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
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
				TimeRanges: []*timeRangeCollectionState{
					{
						From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						To:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					},
					{
						From: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
						To:   time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
					},
					{
						From: time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
						To:   time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC),
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
			t := &TimeRangeSliceCollectionState{
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

func TestTimeRangeSliceCollectionState_IsEmpty(t *testing.T) {
	type fields struct {
		TimeRanges  []*timeRangeCollectionState
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
				TimeRanges:  []*timeRangeCollectionState{},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: true,
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*timeRangeCollectionState{
					{
						From:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						To:              time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"obj1": {}},
						Granularity:     time.Hour,
						CollectionOrder: CollectionOrderChronological,
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
				TimeRanges: []*timeRangeCollectionState{
					{
						From:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						To:              time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"obj1": {}},
						Granularity:     time.Hour,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						From:            time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
						To:              time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"obj2": {}},
						Granularity:     time.Hour,
						CollectionOrder: CollectionOrderChronological,
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
			t := &TimeRangeSliceCollectionState{
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

func TestTimeRangeSliceCollectionState_compact(t *testing.T) {
	tests := []struct {
		name              string
		state             *TimeRangeSliceCollectionState
		expectedRanges    []*timeRangeCollectionState
		currentCollection *timeRange
	}{
		{
			name: "single_range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-20 12:00:00"),
				to:   parseTime("2025-05-30 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological),
			},
		},
		{
			name: "overlapping_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-20 12:00:00"),
				to:   parseTime("2025-05-30 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			},
		},
		{
			name: "non_overlapping_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-20 12:00:00"),
				to:   parseTime("2025-05-30 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			},
		},
		{
			name: "multiple_overlapping_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-13 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-20 12:00:00"),
				to:   parseTime("2025-05-30 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "empty_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-03 12:00:00"),
				to:   parseTime("2025-05-10 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{},
		},
		{
			name: "collection_overlaps_multiple_non_contiguous_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_start_of_first_and_end_of_last_range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-01 12:00:00"),
				to:   parseTime("2025-05-25 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_minute_granularity",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Minute,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Minute, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Minute, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Minute, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_hour_granularity",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_day_granularity",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				24*time.Hour,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_mixed_granularity",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Minute,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Second, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Minute, CollectionOrderChronological, "obj3"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.currentCollectionTimeRange = tt.currentCollection
			tt.state.compact()
			expectedState := buildTimeRangeSliceState(tt.state.Order, tt.state.Granularity, tt.expectedRanges...)
			if equal, msg := timeRangeSliceStateEquals(tt.state, expectedState); !equal {
				t.Error(msg)
			}
		})
	}
}

// TestTimeRangeSliceCollectionState_emulate_collection si,ulates a collection process then verifies the
// resulting collection state
func TestTimeRangeSliceCollectionState_emulate_collection(t *testing.T) {
	tests := []struct {
		name              string
		state             *TimeRangeSliceCollectionState
		currentCollection *timeRange
		expectedState     *TimeRangeSliceCollectionState
	}{
		{
			name: "single_range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-20 12:00:00"),
				to:   parseTime("2025-05-30 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological),
			),
		},
		{
			name: "overlapping_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-20 12:00:00"),
				to:   parseTime("2025-05-30 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
		},
		{
			name: "non_overlapping_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-20 12:00:00"),
				to:   parseTime("2025-05-30 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
			),
		},
		{
			name: "multiple_overlapping_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-13 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-20 12:00:00"),
				to:   parseTime("2025-05-30 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
		},
		{
			name: "empty_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-03 12:00:00"),
				to:   parseTime("2025-05-10 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
			),
		},
		{
			name: "collection_overlaps_multiple_non_contiguous_ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
		},
		{
			name: "collection_overlaps_start_of_first_and_end_of_last_range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-01 12:00:00"),
				to:   parseTime("2025-05-25 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Nanosecond,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, CollectionOrderChronological, "obj3"),
			),
		},
		{
			name: "collection_overlaps_ranges_with_minute_granularity",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Minute,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Minute, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Minute, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Minute,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Minute, CollectionOrderChronological, "obj3"),
			),
		},
		{
			name: "collection_overlaps_ranges_with_hour_granularity",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			),
		},
		{
			name: "collection_overlaps_ranges_with_day_granularity",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				24*time.Hour,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				24*time.Hour,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", 24*time.Hour, CollectionOrderChronological, "obj3"),
			),
		},
		{
			name: "collection_overlaps_ranges_with_mixed_granularity",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Minute,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Second, CollectionOrderChronological, "obj3"),
			),
			currentCollection: &timeRange{
				from: parseTime("2025-05-04 12:00:00"),
				to:   parseTime("2025-05-21 12:00:00"),
			},
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Minute,
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Minute, CollectionOrderChronological, "obj3"),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.currentCollectionTimeRange = tt.currentCollection
			tt.state.compact()
			if equal, msg := timeRangeSliceStateEquals(tt.state, tt.expectedState); !equal {
				t.Error(msg)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_ShouldCollect(t *testing.T) {
	tests := []struct {
		name            string
		state           *TimeRangeSliceCollectionState
		objectTimestamp time.Time
		objectId        string
		want            bool
	}{
		{
			name: "Should collect - no state, timestamp within collection time range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
			),
			objectTimestamp: parseTime("2025-04-15 12:00:00"),
			objectId:        "2025-04-15_120000.txt",
			want:            true,
		},
		{
			name: "Should NOT collect - no state, timestamp outside collection time range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
			),
			objectTimestamp: parseTime("2025-05-01 00:00:00"),
			objectId:        "2025-05-01_000000.txt",
			want:            false,
		},
		{
			name: "Should collect - timestamp within active range but not present in end objects",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
			),
			objectTimestamp: parseTime("2025-04-07 12:00:00"),
			objectId:        "2025-04-07_120000.txt",
			want:            true,
		},
		{
			name: "Should NOT collect - timestamp within active range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
			),
			objectTimestamp: parseTime("2025-04-01 12:00:00"),
			objectId:        "2025-04-01_120000.txt",
			want:            false,
		},
		{
			name: "Should NOT collect - timestamp within active range but present in end objects",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
			),
			objectTimestamp: parseTime("2025-04-07 00:00:00"),
			objectId:        "20250407_000000.txt",
			want:            false,
		},
		{
			name: "Should collect - timestamp within non active (2nd) range but not present in end objects",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: parseTime("2025-04-12 12:00:00"),
			objectId:        "2025-04-12_120000.txt",
			want:            true,
		},
		{
			name: "Should NOT collect - timestamp within non active (2nd) range but present in end objects",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: parseTime("2025-04-12 00:00:00"),
			objectId:        "20250412_000000.txt",
			want:            false,
		},
		{
			name: "Should NOT collect - timestamp within non active (2nd) range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: parseTime("2025-04-10 00:00:00"),
			objectId:        "2025-04-10_000000.txt",
			want:            false,
		},
		{
			name: "Should collect - timestamp within non active (3rd) range but not present in end objects",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: parseTime("2025-04-12 12:00:00"),
			objectId:        "2025-04-12_120000.txt",
			want:            true,
		},
		{
			name: "Should NOT collect - timestamp within non active (3rd) range but present in end objects",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: parseTime("2025-04-12 00:00:00"),
			objectId:        "20250412_000000.txt",
			want:            false,
		},
		{
			name: "Should NOT collect - timestamp within non active (3rd) range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, CollectionOrderChronological, "20250412_000000.txt"),
			),
			objectTimestamp: parseTime("2025-04-10 00:00:00"),
			objectId:        "2025-04-10_000000.txt",
			want:            false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.OnCollectionStarted(parseTime("2025-04-01 00:00:00"), parseTime("2025-04-30 00:00:00"))
			if got := tt.state.ShouldCollect(tt.objectId, tt.objectTimestamp); got != tt.want {
				t.Errorf("ShouldCollect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_updateActiveRange(t *testing.T) {
	tests := []struct {
		name          string
		state         *TimeRangeSliceCollectionState
		timestamp     time.Time
		expectedState *TimeRangeSliceCollectionState
	}{
		{
			name: "No existing ranges - check initial active range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
			),
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "Existing range - extend active range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: parseTime("2025-04-08 00:00:00"),
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "Multiple ranges, timestamp in between ranges - extend initial active range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: parseTime("2025-04-09 00:00:00"),
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "Multiple ranges, timestamp in 2nd range ranges - update active range and set original active range end time",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: parseTime("2025-04-03 00:00:00"),
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-03 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// call OnCollectionStarted to initialize the active range
			tt.state.OnCollectionStarted(parseTime("2025-04-01 00:00:00"), parseTime("2025-04-30 00:00:00"))

			// if there is a timestamp, update the active range
			if !tt.timestamp.IsZero() {
				tt.state.updateActiveRange(tt.timestamp)
			}

			if equal, msg := timeRangeSliceStateEquals(tt.state, tt.expectedState); !equal {
				t.Error(msg)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_upperBoundaryTime(t1 *testing.T) {
	tests := []struct {
		name  string
		state *TimeRangeSliceCollectionState
		want  time.Time
	}{
		{
			name:  "empty_ranges_chronological",
			state: buildTimeRangeSliceState(CollectionOrderChronological, time.Hour),
			want:  time.Time{},
		},
		{
			name:  "empty_ranges_reverse",
			state: buildTimeRangeSliceState(CollectionOrderReverse, time.Hour),
			want:  time.Time{},
		},
		{
			name: "single_range_chronological",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "single_range_reverse",
			state: buildTimeRangeSliceState(
				CollectionOrderReverse,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderReverse),
			),
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "multiple_ranges_chronological",
			state: buildTimeRangeSliceState(
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
			state: buildTimeRangeSliceState(
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
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := tt.state.upperBoundaryTime(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("upperBoundaryTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_lowerBoundaryTime(t1 *testing.T) {
	tests := []struct {
		name  string
		state *TimeRangeSliceCollectionState
		want  time.Time
	}{
		{
			name:  "empty_ranges_chronological",
			state: buildTimeRangeSliceState(CollectionOrderChronological, time.Hour),
			want:  time.Time{},
		},
		{
			name:  "empty_ranges_reverse",
			state: buildTimeRangeSliceState(CollectionOrderReverse, time.Hour),
			want:  time.Time{},
		},
		{
			name: "single_range_chronological",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			want: parseTime("2024-01-01 00:00:00"),
		},
		{
			name: "single_range_reverse",
			state: buildTimeRangeSliceState(
				CollectionOrderReverse,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderReverse),
			),
			want: parseTime("2024-01-02 00:00:00"),
		},
		{
			name: "multiple_ranges_chronological",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			want: parseTime("2024-01-01 00:00:00"),
		},
		{
			name: "multiple_ranges_reverse",
			state: buildTimeRangeSliceState(
				CollectionOrderReverse,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderReverse),
			),
			want: parseTime("2024-01-06 00:00:00"),
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

func TestTimeRangeSliceCollectionState_rangeForTime(t1 *testing.T) {

	tests := []struct {
		name      string
		state     *TimeRangeSliceCollectionState
		timestamp time.Time
		want      *timeRangeCollectionState
	}{
		{
			name:      "empty state",
			state:     buildTimeRangeSliceState(CollectionOrderChronological, time.Hour),
			timestamp: parseTime("2024-01-01 00:00:00"),
			want:      nil,
		},
		{
			name: "single range - within range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: parseTime("2024-01-01 12:00:00"),
			want:      buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "single range - at start time",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: parseTime("2024-01-01 00:00:00"),
			want:      buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "single range - at end time",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: parseTime("2024-01-02 00:00:00"),
			want:      buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "single range - outside range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: parseTime("2024-01-03 00:00:00"),
			want:      nil,
		},
		{
			name: "multiple ranges - within first range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: parseTime("2024-01-01 12:00:00"),
			want:      buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "multiple ranges - within middle range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: parseTime("2024-01-03 12:00:00"),
			want:      buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "multiple ranges - within last range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: parseTime("2024-01-05 12:00:00"),
			want:      buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
		},
		{
			name: "multiple ranges - between ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: parseTime("2024-01-02 12:00:00"),
			want:      nil,
		},
		{
			name: "multiple ranges - reverse order",
			state: buildTimeRangeSliceState(
				CollectionOrderReverse,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderReverse),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderReverse),
			),
			timestamp: parseTime("2024-01-03 12:00:00"),
			want:      buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderReverse),
		},
		{
			name: "multiple ranges - with end objects",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2024-01-01 00:00:00", "2024-01-02 00:00:00", time.Hour, CollectionOrderChronological, "obj1"),
				buildTimeRangeState("2024-01-03 00:00:00", "2024-01-04 00:00:00", time.Hour, CollectionOrderChronological, "obj2"),
				buildTimeRangeState("2024-01-05 00:00:00", "2024-01-06 00:00:00", time.Hour, CollectionOrderChronological, "obj3"),
			),
			timestamp: parseTime("2024-01-03 12:00:00"),
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

func TestTimeRangeSliceCollectionState_addRange(t1 *testing.T) {
	tests := []struct {
		name          string
		state         *TimeRangeSliceCollectionState
		timestamp     time.Time
		expectedState *TimeRangeSliceCollectionState
	}{
		{
			name: "empty state - add first range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
			),
			timestamp: parseTime("2025-04-01 00:00:00"),
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "single range - add new range after",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: parseTime("2025-04-08 00:00:00"),
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-08 00:00:00", "2025-04-08 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "multiple ranges - add new range after existing ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: parseTime("2025-04-05 00:00:00"),
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-05 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "multiple ranges - add new range between existing ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-04 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: parseTime("2025-04-03 00:00:00"),
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-03 00:00:00", "2025-04-03 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-04 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "multiple ranges - add new range before existing ranges",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-02 00:00:00", "2025-04-03 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-04 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
			timestamp: parseTime("2025-04-01 00:00:00"),
			expectedState: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour*24,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-02 00:00:00", "2025-04-03 00:00:00", time.Hour*24, CollectionOrderChronological),
				buildTimeRangeState("2025-04-04 00:00:00", "2025-04-05 00:00:00", time.Hour*24, CollectionOrderChronological),
			),
		},
		{
			name: "different granularity - add new range",
			state: buildTimeRangeSliceState(
				CollectionOrderChronological,
				time.Hour,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 01:00:00", time.Hour, CollectionOrderChronological),
				buildTimeRangeState("2025-04-01 02:00:00", "2025-04-01 03:00:00", time.Hour, CollectionOrderChronological),
			),
			timestamp: parseTime("2025-04-01 01:30:00"),
			expectedState: buildTimeRangeSliceState(
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
			if equal, msg := timeRangeSliceStateEquals(tt.state, tt.expectedState); !equal {
				t.Error(msg)
			}
		})
	}
}

func buildTimeRangeSliceState(order CollectionOrder, granularity time.Duration, ranges ...*timeRangeCollectionState) *TimeRangeSliceCollectionState {
	return &TimeRangeSliceCollectionState{
		TimeRanges:     ranges,
		Granularity:    granularity,
		objectRangeMap: map[string]*timeRangeCollectionState{},
		Order:          order,
	}
}

func timeRangeSliceStateEquals(got, want *TimeRangeSliceCollectionState) (bool, string) {
	if len(got.TimeRanges) != len(want.TimeRanges) {
		return false, fmt.Sprintf("range count = %v, want %v", len(got.TimeRanges), len(want.TimeRanges))
	}
	for i, expected := range want.TimeRanges {
		if i >= len(got.TimeRanges) {
			return false, fmt.Sprintf("missing range at index %v", i)
		}
		if equal, msg := timeRangeStateEquals(got.TimeRanges[i], expected); !equal {
			return false, msg
		}
	}
	return true, ""
}
