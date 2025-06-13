package collection_state

import (
	"reflect"
	"testing"
	"time"

	"github.com/turbot/go-kit/helpers"
)

func TestTimeRangeSliceCollectionState_GetEndTime(t *testing.T) {
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
			want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
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
			want: time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC),
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

func TestTimeRangeSliceCollectionState_GetStartTime(t *testing.T) {
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
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 20, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 30, 12, 0, 0, 0, time.UTC),
				},
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond),
			},
		},
		{
			name: "overlapping_ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, "obj1"),
					buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
				},
				// collection from/to does not overlap multiple ranges - so will have no affect on the compacting
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 20, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 30, 12, 0, 0, 0, time.UTC),
				},
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
			},
		},
		{
			name: "non_overlapping_ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, "obj1"),
					buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
				},
				// collection from/to does not overlap multiple ranges - so will have no affect on the compacting
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 20, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 30, 12, 0, 0, 0, time.UTC),
				},
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, "obj1"),
				buildTimeRangeState("2025-05-11 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
			},
		},
		{
			name: "multiple_overlapping_ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond, "obj1"),
					buildTimeRangeState("2025-05-08 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
					buildTimeRangeState("2025-05-13 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, "obj3"),
				},
				// collection from/to does not overlap multiple ranges - so will have no affect on the compacting
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 20, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 30, 12, 0, 0, 0, time.UTC),
				},
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-20 12:00:00", time.Nanosecond, "obj3"),
			},
		},
		{
			name: "empty_ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{},
			},
			expectedRanges: []*timeRangeCollectionState{},
			currentCollection: &timeRange{
				from: time.Date(2025, 5, 3, 12, 0, 0, 0, time.UTC),
				to:   time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "collection_overlaps_multiple_non_contiguous_ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, "obj1"),
					buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
					buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, "obj3"),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 4, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 21, 12, 0, 0, 0, time.UTC),
				},
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, "obj3"),
			},
		},
		{
			name: "collection_overlaps_start_of_first_and_end_of_last_range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, "obj1"),
					buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
					buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, "obj3"),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 1, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 25, 12, 0, 0, 0, time.UTC),
				},
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, "obj3"),
			},
		},
		{
			name: "collection_overlaps_middle_ranges_only",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, "obj1"),
					buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
					buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, "obj3"),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 6, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 19, 12, 0, 0, 0, time.UTC),
				},
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, "obj1"),
				buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, "obj3"),
			},
		},
		{
			name: "collection_overlaps_partial_ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Nanosecond, "obj1"),
					buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
					buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, "obj3"),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 4, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 14, 12, 0, 0, 0, time.UTC),
				},
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-15 12:00:00", time.Nanosecond, "obj2"),
				buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Nanosecond, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_second_granularity",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Second, "obj1"),
					buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Second, "obj2"),
					buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Second, "obj3"),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 4, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 21, 12, 0, 0, 0, time.UTC),
				},
				Granularity: time.Second,
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Second, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_minute_granularity",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, "obj1"),
					buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Minute, "obj2"),
					buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Minute, "obj3"),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 4, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 21, 12, 0, 0, 0, time.UTC),
				},
				Granularity: time.Minute,
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Minute, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_hour_granularity",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Hour, "obj1"),
					buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, "obj2"),
					buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Hour, "obj3"),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 4, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 21, 12, 0, 0, 0, time.UTC),
				},
				Granularity: time.Hour,
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Hour, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_day_granularity",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", 24*time.Hour, "obj1"),
					buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", 24*time.Hour, "obj2"),
					buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", 24*time.Hour, "obj3"),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 4, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 21, 12, 0, 0, 0, time.UTC),
				},
				Granularity: 24 * time.Hour,
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", 24*time.Hour, "obj3"),
			},
		},
		{
			name: "collection_overlaps_ranges_with_mixed_granularity",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-01 12:00:00", "2025-05-05 12:00:00", time.Minute, "obj1"),
					buildTimeRangeState("2025-05-10 12:00:00", "2025-05-15 12:00:00", time.Hour, "obj2"),
					buildTimeRangeState("2025-05-20 12:00:00", "2025-05-25 12:00:00", time.Second, "obj3"),
				},
				currentCollectionTimeRange: &timeRange{
					from: time.Date(2025, 5, 4, 12, 0, 0, 0, time.UTC),
					to:   time.Date(2025, 5, 21, 12, 0, 0, 0, time.UTC),
				},
				Granularity: time.Minute,
			},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 12:00:00", "2025-05-25 12:00:00", time.Minute, "obj3"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.compact()
			if len(tt.state.TimeRanges) != len(tt.expectedRanges) {
				t.Errorf("compact() range count = %v, want %v", len(tt.state.TimeRanges), len(tt.expectedRanges))
			}
			for i, expected := range tt.expectedRanges {
				if i >= len(tt.state.TimeRanges) {
					t.Errorf("compact() missing range at index %v", i)
					continue
				}
				if equal, msg := timeRangeStateEquals(tt.state.TimeRanges[i], expected); !equal {
					t.Error(msg)
				}
			}
		})
	}
}

// TestTimeRangeSliceCollectionState_emulate_collection si,ulates a collection process then verifies the
// resulting collection state
func TestTimeRangeSliceCollectionState_emulate_collection(t *testing.T) {
	// 'to' defaults to 'now' (2025-05-10 12:00:00 UTC)
	defaultTo := time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC)
	// 'from' defaults to last 7 days
	defaultFrom := time.Date(2025, 5, 3, 12, 0, 0, 0, time.UTC)
	granularity := time.Hour

	tests := []struct {
		name  string
		state *TimeRangeSliceCollectionState
		from  time.Time
		to    time.Time
		// optional - a list of days with no data
		emptyDays      []int
		expectedRanges []*timeRangeCollectionState
	}{
		{
			name:  "First Collection - Defaults (No Parameters)",
			state: nil,
			from:  defaultFrom,
			to:    defaultTo,
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", granularity),
			},
		},
		{
			name:  "First Collection - Default From, Custom To",
			state: nil,
			from:  defaultFrom,
			to:    time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-05 00:00:00", granularity),
			},
		},
		{
			name:  "First Collection - Custom From, Default To",
			state: nil,
			from:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			to:    defaultTo,
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 00:00:00", "2025-05-10 12:00:00", granularity),
			},
		},
		{
			name:  "First Collection - Custom From and To",
			state: nil,
			from:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 00:00:00", "2025-05-05 00:00:00", granularity),
			},
		},
		{
			name: "One Existing Range - Defaults (No Parameters)",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt"),
			),
			// end time of range
			from: time.Date(2025, 04, 26, 0, 0, 0, 0, time.UTC),
			to:   defaultTo,
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-10 12:00:00", granularity),
			},
		},
		{
			name:  "One Existing Range - Collection of Non-Adjacent Range",
			state: buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:  time.Date(2025, 4, 28, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt"),
				buildTimeRangeState("2025-04-28 00:00:00", "2025-05-03 00:00:00", granularity),
			},
		},
		{
			name:  "One Existing Range - Collection of Adjacent Range",
			state: buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:  time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-03 00:00:00", granularity),
			},
		},
		{
			name:  "One Existing Range - Collection Encompasses Existing Range",
			state: buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-15 00:00:00", "2025-05-01 00:00:00", granularity),
			},
		},
		{
			name:  "One Existing Range - Collection Overlaps Beginning of Existing Range",
			state: buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 4, 20, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt"),
			},
		},
		{
			name: "One Existing Range - Collection Overlaps End of Existing Range",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt"),
			),
			from: time.Date(2025, 4, 24, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-01 00:00:00", granularity),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Between Two Ranges",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Overlapping End of First Range and Start of Second Range",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 17, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Encompassing Multiple Ranges",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", granularity, "20250412_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-03-25 00:00:00", "2025-04-25 00:00:00", granularity),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Partially Overlapping Multiple Ranges",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 17, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Before All Existing Ranges",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-03-15 00:00:00", "2025-03-25 00:00:00", granularity),
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection After All Existing Ranges",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
				buildTimeRangeState("2025-04-25 00:00:00", "2025-05-01 00:00:00", granularity),
			},
		},
		{
			name: "Multiple Existing Ranges - Default Collection Parameters",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-05-10 12:00:00", granularity),
			},
		},
		{
			name: "Multiple Existing Ranges - Creating New Gap Between Ranges",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-20 00:00:00", "2025-04-27 00:00:00", granularity, "20250427_000000.txt"),
			),
			from: time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-15 00:00:00", granularity),
				buildTimeRangeState("2025-04-20 00:00:00", "2025-04-27 00:00:00", granularity, "20250427_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Overlapping Only Some Ranges",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", granularity, "20250412_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 11, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-12 00:00:00", granularity, "20250412_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Creating Multiple New Gaps",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
				buildTimeRangeState("2025-04-25 00:00:00", "2025-04-30 00:00:00", granularity, "20250430_000000.txt"),
			),
			from: time.Date(2025, 4, 23, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 24, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
				buildTimeRangeState("2025-04-23 00:00:00", "2025-04-24 00:00:00", granularity),
				buildTimeRangeState("2025-04-25 00:00:00", "2025-04-30 00:00:00", granularity, "20250430_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Exactly Adjacent to Multiple Ranges",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Partial Overlap at Exact Boundaries",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 18, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},

		{
			name:      "First Collection - Defaults (No Parameters) - No Data",
			state:     nil,
			from:      defaultFrom,
			to:        defaultTo,
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", granularity),
			},
		},
		{
			name:      "First Collection - Default From, Custom To - No Data",
			state:     nil,
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			from:      defaultFrom,
			to:        time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-05 00:00:00", granularity),
			},
		},
		{
			name:  "First Collection - Custom From, Default To",
			state: nil,
			from:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			to:    defaultTo,
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 00:00:00", "2025-05-10 12:00:00", granularity),
			},
		},
		{
			name:      "First Collection - Custom From and To - No Data",
			state:     nil,
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			from:      time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 00:00:00", "2025-05-05 00:00:00", granularity),
			},
		},
		{
			name: "One Existing Range - Defaults (No Parameters) - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt"),
			),
			// end time of range
			from:      time.Date(2025, 04, 26, 0, 0, 0, 0, time.UTC),
			to:        defaultTo,
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-10 12:00:00", granularity),
			},
		},
		{
			name:      "One Existing Range - Collection of Non-Adjacent Range - No Data",
			state:     buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:      time.Date(2025, 4, 28, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt"),
				buildTimeRangeState("2025-04-28 00:00:00", "2025-05-03 00:00:00", granularity),
			},
		},
		{
			name:      "One Existing Range - Collection of Adjacent Range - No Data",
			state:     buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:      time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-03 00:00:00", granularity),
			},
		},
		{
			name:      "One Existing Range - Collection Encompasses Existing Range - No Data",
			state:     buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:      time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-15 00:00:00", "2025-05-01 00:00:00", granularity),
			},
		},
		{
			name:      "One Existing Range - Collection Overlaps Beginning of Existing Range - No Data",
			state:     buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:      time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 4, 20, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt"),
			},
		},
		{
			name: "One Existing Range - Collection Overlaps End of Existing Range - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt"),
			),
			from:      time.Date(2025, 4, 24, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-01 00:00:00", granularity),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Between Two Ranges - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from:      time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Overlapping End of First Range and Start of Second Range - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from:      time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 4, 17, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Encompassing Multiple Ranges - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", granularity, "20250412_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from:      time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-03-25 00:00:00", "2025-04-25 00:00:00", granularity),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Partially Overlapping Multiple Ranges - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from:      time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 4, 17, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Before All Existing Ranges - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from:      time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-03-15 00:00:00", "2025-03-25 00:00:00", granularity),
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Collection After All Existing Ranges - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from:      time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
				buildTimeRangeState("2025-04-25 00:00:00", "2025-05-01 00:00:00", granularity),
			},
		},
		{
			name: "Multiple Existing Ranges - Default Collection Parameters - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from:      time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-05-10 12:00:00", granularity),
			},
		},
		{
			name: "Multiple Existing Ranges - Creating New Gap Between Ranges - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-20 00:00:00", "2025-04-27 00:00:00", granularity, "20250427_000000.txt"),
			),
			from:      time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-15 00:00:00", granularity),
				buildTimeRangeState("2025-04-20 00:00:00", "2025-04-27 00:00:00", granularity, "20250427_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Overlapping Only Some Ranges - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", granularity, "20250412_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 11, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-12 00:00:00", granularity, "20250412_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Creating Multiple New Gaps - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
				buildTimeRangeState("2025-04-25 00:00:00", "2025-04-30 00:00:00", granularity, "20250430_000000.txt"),
			),
			from:      time.Date(2025, 4, 23, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 4, 24, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
				buildTimeRangeState("2025-04-23 00:00:00", "2025-04-24 00:00:00", granularity),
				buildTimeRangeState("2025-04-25 00:00:00", "2025-04-30 00:00:00", granularity, "20250430_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Exactly Adjacent to Multiple Ranges - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from:      time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
		{
			name: "Multiple Existing Ranges - Partial Overlap at Exact Boundaries - No Data",
			state: buildTimeRangeSliceState(granularity,
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", granularity, "20250407_000000.txt"),
				buildTimeRangeState("2025-04-15 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			),
			from:      time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2025, 4, 18, 0, 0, 0, 0, time.UTC),
			emptyDays: []int{3, 4, 5, 6, 7, 8, 9},
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-01 00:00:00", "2025-04-22 00:00:00", granularity, "20250422_000000.txt"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize the state if not provided
			if tt.state == nil {
				tt.state = NewTimeRangeSliceCollectionState(&timeRange{tt.from, tt.to}, CollectionOrderChronological)
				tt.state.SetGranularity(granularity)
			} else {
				tt.state.OnCollectionStarted(tt.from, tt.to)
			}
			// convert emptyDays to a map for quick lookup
			emptyDaysMap := helpers.SliceToLookup(tt.emptyDays)

			// Simulate the collection process
			for fileTime := tt.from; !fileTime.After(tt.to); fileTime = fileTime.Add(time.Minute) {
				if _, isEmpty := emptyDaysMap[fileTime.Day()]; isEmpty {
					continue // Skip empty days
				}

				// Only get a file every 15 mins past hour (simplicity)
				if fileTime.Minute()%15 != 0 {
					continue
				}
				fileName := fileTime.Format("20060102_150405.txt")

				if tt.state.ShouldCollect(fileName, fileTime) {
					err := tt.state.OnCollected(fileName, fileTime)
					if err != nil {
						t.Errorf("Error collecting file %s at time %s: %v", fileName, fileTime, err)
					}
				}
			}

			// The collection completed successfully
			_ = tt.state.OnCollectionComplete()

			// Check the number of ranges after compaction
			if len(tt.state.TimeRanges) != len(tt.expectedRanges) {
				t.Fatalf("Expected %d ranges after compaction, got %d", len(tt.expectedRanges), len(tt.state.TimeRanges))
			}
			// Check if the ranges match by comparing each field
			for i, actual := range tt.state.TimeRanges {
				expected := tt.expectedRanges[i]

				// Compare From
				if !actual.From.Equal(expected.From) {
					t.Errorf("Range %d: From mismatch - expected: %v, got: %v", i, expected.From, actual.From)
				}

				// Compare To
				if !actual.To.Equal(expected.To) {
					t.Errorf("Range %d: To mismatch - expected: %v, got: %v", i, expected.To, actual.To)
				}

				// Compare EndObjects
				if !reflect.DeepEqual(actual.EndObjects, expected.EndObjects) {
					t.Errorf("Range %d: EndObjects mismatch - expected: %v, got: %v", i, expected.EndObjects, actual.EndObjects)
				}
			}

		})
	}
}

func buildTimeRangeSliceState(granularity time.Duration, ranges ...*timeRangeCollectionState) *TimeRangeSliceCollectionState {

	return &TimeRangeSliceCollectionState{
		TimeRanges:     ranges,
		Granularity:    granularity,
		objectRangeMap: map[string]*timeRangeCollectionState{},
		Order:          CollectionOrderChronological,
	}
}

func TestTimeRangeSliceCollectionState_ShouldCollect(t *testing.T) {
	type args struct {
		state           *TimeRangeSliceCollectionState
		granularity     time.Duration
		from            time.Time
		to              time.Time
		objectTimestamp time.Time
		objectId        string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Should collect - no state, timestamp within collection time range",
			args: args{
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				granularity:     time.Hour * 24,
				objectTimestamp: time.Date(2025, 4, 15, 12, 0, 0, 0, time.UTC),
				objectId:        "2025-04-15_120000.txt",
			},
			want: true,
		},
		{
			name: "Should NOT collect - no state, timestamp outside collection time range",
			args: args{
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				granularity:     time.Hour * 24,
				objectTimestamp: time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
				objectId:        "2025-05-01_000000.txt",
			},
			want: false,
		},
		{
			name: "Should collect - timestamp within active range but not present in end objects",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, "20250407_000000.txt"),
				),
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				objectTimestamp: time.Date(2025, 4, 07, 12, 0, 0, 0, time.UTC),
				objectId:        "2025-04-07_120000.txt",
				granularity:     time.Hour * 24,
			},
			want: true,
		},
		{
			name: "Should NOT collect - timestamp within active range",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, "20250407_000000.txt"),
				),
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				objectTimestamp: time.Date(2025, 4, 01, 12, 0, 0, 0, time.UTC),
				objectId:        "2025-04-01_120000.txt",
				granularity:     time.Hour * 24,
			},
			want: false,
		},
		{
			name: "Should NOT collect - timestamp within active range but present in end objects",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, "20250407_000000.txt"),
				),
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				objectTimestamp: time.Date(2025, 4, 7, 00, 0, 0, 0, time.UTC),
				objectId:        "20250407_000000.txt",
				granularity:     time.Hour * 24,
			},
			want: false,
		},

		{
			name: "Should collect - timestamp within non active (2nd) range but not present in end objects",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, "20250407_000000.txt"),
					buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, "20250412_000000.txt"),
				),
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				objectTimestamp: time.Date(2025, 4, 12, 12, 0, 0, 0, time.UTC),
				objectId:        "2025-04-12_120000.txt",
				granularity:     time.Hour * 24,
			},
			want: true,
		},
		{
			name: "Should NOT collect - timestamp within non active (2nd) range but present in end objects",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, "20250407_000000.txt"),
					buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, "20250412_000000.txt"),
				),
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				objectTimestamp: time.Date(2025, 4, 12, 0, 0, 0, 0, time.UTC),
				objectId:        "20250412_000000.txt",
				granularity:     time.Hour * 24,
			},
			want: false,
		},
		{
			name: "Should NOT collect - timestamp within non active (2nd) range",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24, "20250407_000000.txt"),
					buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, "20250412_000000.txt"),
				),
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				objectTimestamp: time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
				objectId:        "2025-04-10_000000.txt",
				granularity:     time.Hour * 24,
			},
			want: false,
		},
		{
			name: "Should collect - timestamp within non active (3rd) range but not present in end objects",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-04 00:00:00", time.Hour*24, "20250407_000000.txt"),
					buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, "20250407_000000.txt"),
					buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, "20250412_000000.txt"),
				),
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				objectTimestamp: time.Date(2025, 4, 12, 12, 0, 0, 0, time.UTC),
				objectId:        "2025-04-12_120000.txt",
				granularity:     time.Hour * 24,
			},
			want: true,
		},
		{
			name: "Should NOT collect - timestamp within non active (3rd) range but present in end objects",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-04 00:00:00", time.Hour*24, "20250407_000000.txt"),
					buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, "20250407_000000.txt"),
					buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, "20250412_000000.txt"),
				),
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				objectTimestamp: time.Date(2025, 4, 12, 0, 0, 0, 0, time.UTC),
				objectId:        "20250412_000000.txt",
				granularity:     time.Hour * 24,
			},
			want: false,
		},
		{
			name: "Should NOT collect - timestamp within non active (3rd) range",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-04 00:00:00", time.Hour*24, "20250407_000000.txt"),
					buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24, "20250407_000000.txt"),
					buildTimeRangeState("2025-04-10 00:00:00", "2025-04-12 00:00:00", time.Hour*24, "20250412_000000.txt"),
				),
				from:            time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
				to:              time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
				objectTimestamp: time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
				objectId:        "2025-04-10_000000.txt",
				granularity:     time.Hour * 24,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {

			// Initialize the state if not provided
			if tt.args.state == nil {
				tt.args.state = NewTimeRangeSliceCollectionState(&timeRange{tt.args.from, tt.args.to}, CollectionOrderChronological)
				tt.args.state.SetGranularity(tt.args.granularity)
			} else {
				tt.args.state.OnCollectionStarted(tt.args.from, tt.args.to)
			}

			if got := tt.args.state.ShouldCollect(tt.args.objectId, tt.args.objectTimestamp); got != tt.want {
				t1.Errorf("ShouldCollect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_updateActiveRange(t *testing.T) {
	from := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC)
	type args struct {
		timestamp   time.Time
		granularity time.Duration
		state       *TimeRangeSliceCollectionState
	}
	type want struct {
		active    *timeRangeCollectionState
		allRanges []*timeRangeCollectionState
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "No existing ranges - check  initial active range",
			args: args{
				state:       buildTimeRangeSliceState(time.Hour * 24),
				granularity: time.Hour * 24,
			},
			want: want{
				active:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 00:00:00", time.Hour*24),
				allRanges: []*timeRangeCollectionState{buildTimeRangeState("2025-04-01 00:00:00", "2025-04-01 00:00:00", time.Hour*24)},
			},
		},
		{
			name: "Existing range - extend active range",
			args: args{
				state:       buildTimeRangeSliceState(time.Hour*24, buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24)),
				timestamp:   time.Date(2025, 4, 8, 0, 0, 0, 0, time.UTC),
				granularity: time.Hour * 24,
			},
			// use original range time - the range will not be updated until OnCollected is called
			want: want{
				active:    buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24),
				allRanges: []*timeRangeCollectionState{buildTimeRangeState("2025-04-01 00:00:00", "2025-04-07 00:00:00", time.Hour*24)},
			},
		},
		{
			name: "Multiple ranges, timestamp in between ranges - extend	 initial active range",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24),
					buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24),
					buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24)),

				timestamp:   time.Date(2025, 4, 9, 0, 0, 0, 0, time.UTC),
				granularity: time.Hour * 24,
			},
			// use original range time - the range will not be updated until OnCollected is called
			want: want{
				active: buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24),
				allRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24),
					buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24),
					buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24),
				},
			},
		},
		{
			name: "Multiple ranges, timestamp in 2nd range ranges - update active range and set original active range end time",
			args: args{
				state: buildTimeRangeSliceState(
					time.Hour*24,
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-02 00:00:00", time.Hour*24),
					buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24),
					buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24)),

				timestamp:   time.Date(2025, 4, 3, 0, 0, 0, 0, time.UTC),
				granularity: time.Hour * 24,
			},
			// use original range time - the range will not be updated until OnCollected is called
			want: want{
				active: buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24),
				allRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-04-01 00:00:00", "2025-04-03 00:00:00", time.Hour*24),
					buildTimeRangeState("2025-04-03 00:00:00", "2025-04-04 00:00:00", time.Hour*24),
					buildTimeRangeState("2025-04-06 00:00:00", "2025-04-07 00:00:00", time.Hour*24),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			// call OnCollectionStarted to initialize the active range
			tt.args.state.OnCollectionStarted(from, to)

			// if there is a timesampe, update the active range
			if !tt.args.timestamp.IsZero() {
				tt.args.state.updateActiveRange(tt.args.timestamp)
			}
			// now check the active range
			got := tt.args.state.activeRange
			if equal, msg := timeRangeStateEquals(got, tt.want.active); !equal {
				t1.Error(msg)
			}
			// check all ranges
			if len(tt.args.state.TimeRanges) != len(tt.want.allRanges) {
				t1.Errorf("Expected %d ranges, got %d", len(tt.want.allRanges), len(tt.args.state.TimeRanges))
			}
			for i, r := range tt.args.state.TimeRanges {
				if equal, msg := timeRangeStateEquals(r, tt.want.allRanges[i]); !equal {
					t1.Errorf("Range %d mismatch: %s", i, msg)
				}
			}
		})
	}
}
