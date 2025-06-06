package collection_state

import (
	"reflect"
	"testing"
	"time"
)

func TestTimeRangeSliceCollectionState_GetEndTime(t1 *testing.T) {
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
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.GetEndTime(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("GetEndTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_GetStartTime(t1 *testing.T) {
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
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.GetStartTime(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("GetStartTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeRangeSliceCollectionState_IsEmpty(t1 *testing.T) {
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
		t1.Run(tt.name, func(t1 *testing.T) {
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
		currentCollection *collectionMetadata
	}{
		{
			name: "single_range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", time.Nanosecond),
				},
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
			currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				currentCollection: &collectionMetadata{
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
				if equal, msg := timeRangeStateEquals(tt.state.TimeRanges[i], expected, i); !equal {
					t.Error(msg)
				}
			}
		})
	}
}

func TestTimeRangeSliceCollectionState(t *testing.T) {
	// 'to' defualts to 'now'
	defaultTo := time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC)
	// 'from' defaults to last 7 days
	defaultFrom := defaultTo.Add(-time.Hour * 24 * 7) // default from 7 days before defaultTo
	granularity := time.Hour

	tests := []struct {
		name           string
		state          *TimeRangeSliceCollectionState
		from           time.Time
		to             time.Time
		expectedRanges []*timeRangeCollectionState
	}{
		{
			name:  "First Collection - Defaults (No Parameters)",
			state: nil,
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-10 12:00:00", granularity, "20250510_120000.txt"),
			},
		},
		{
			name:  "First Collection - Default From, Custom To",
			state: nil,
			to:    time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-03 12:00:00", "2025-05-05 00:00:00", granularity, "20250505_000000.txt"),
			},
		},
		{
			name:  "First Collection - Custom From, Default To",
			state: nil,
			from:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 00:00:00", "2025-05-10 12:00:00", granularity, "20250510_120000.txt"),
			},
		},
		{
			name:  "First Collection - Custom From and To",
			state: nil,
			from:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-05-01 00:00:00", "2025-05-05 00:00:00", granularity, "20250505_000000.txt"),
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
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-10 12:00:00", granularity, "20250510_120000.txt"),
			},
		},
		{
			name:  "One Existing Range - Collection of Non-Adjacent Range",
			state: buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:  time.Date(2025, 4, 28, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt"),
				buildTimeRangeState("2025-04-28 00:00:00", "2025-05-03 00:00:00", granularity, "20250503_000000.txt"),
			},
		},
		{
			name:  "One Existing Range - Collection of Adjacent Range",
			state: buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:  time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-03 00:00:00", granularity, "20250503_000000.txt"),
			},
		},
		{
			name:  "One Existing Range - Collection Encompasses Existing Range",
			state: buildTimeRangeSliceState(granularity, buildTimeRangeState("2025-04-19 12:00:00", "2025-04-26 00:00:00", granularity, "20250426_000000.txt")),
			from:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				buildTimeRangeState("2025-04-15 00:00:00", "2025-05-01 00:00:00", granularity, "20250501_000000.txt"),
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
				buildTimeRangeState("2025-04-19 12:00:00", "2025-05-01 00:00:00", granularity, "20250501_000000.txt"),
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
				buildTimeRangeState("2025-03-25 00:00:00", "2025-04-25 00:00:00", granularity, "20250425_000000.txt"),
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
				buildTimeRangeState("2025-03-15 00:00:00", "2025-03-25 00:00:00", granularity, "20250325_000000.txt"),
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
				buildTimeRangeState("2025-04-25 00:00:00", "2025-05-01 00:00:00", granularity, "20250501_000000.txt"),
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
				buildTimeRangeState("2025-04-15 00:00:00", "2025-05-10 12:00:00", granularity, "20250510_120000.txt"),
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
				buildTimeRangeState("2025-04-10 00:00:00", "2025-04-15 00:00:00", granularity, "20250415_000000.txt"),
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
				buildTimeRangeState("2025-04-23 00:00:00", "2025-04-24 00:00:00", granularity, "20250424_000000.txt"),
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			from := tt.from
			if from.IsZero() {
				from = defaultFrom
			}
			to := tt.to
			if to.IsZero() {
				to = defaultTo
			}

			// Initialize the state if not provided
			if tt.state == nil {
				tt.state = NewTimeRangeSliceCollectionState(&collectionMetadata{from, to}, CollectionOrderChronological)
				tt.state.SetGranularity(granularity)
			} else {
				tt.state.OnCollectionStarted(from, to)
			}

			// Simulate the collection process
			for fileTime := from; !fileTime.After(to); fileTime = fileTime.Add(time.Minute) {
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

			// Assume the collection completed successfully so compact
			tt.state.compact()

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
