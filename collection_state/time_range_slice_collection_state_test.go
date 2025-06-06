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
						firstEntryTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:  time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						endTime:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
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
						firstEntryTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:  time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						endTime:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					},
					{
						firstEntryTime: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
						lastEntryTime:  time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
						endTime:        time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
					},
					{
						firstEntryTime: time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
						lastEntryTime:  time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC),
						endTime:        time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC),
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

func TestTimeRangeSliceCollectionState_GetGranularity(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*timeRangeCollectionState
		Granularity time.Duration
		Order       CollectionOrder
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Duration
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			if got := t.GetGranularity(); got != tt.want {
				t1.Errorf("GetGranularity() = %v, want %v", got, tt.want)
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
						firstEntryTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:  time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						endTime:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
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
						firstEntryTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:  time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						endTime:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					},
					{
						firstEntryTime: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
						lastEntryTime:  time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
						endTime:        time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
					},
					{
						firstEntryTime: time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
						lastEntryTime:  time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC),
						endTime:        time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC),
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
						firstEntryTime:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
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
						firstEntryTime:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"obj1": {}},
						Granularity:     time.Hour,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
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

func TestTimeRangeSliceCollectionState_SetEndTime(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*timeRangeCollectionState
		Granularity time.Duration
		Order       CollectionOrder
	}
	type args struct {
		endTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			t.SetEndTime(tt.args.endTime)
		})
	}
}

func TestTimeRangeSliceCollectionState_SetGranularity(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*timeRangeCollectionState
		Granularity time.Duration
		Order       CollectionOrder
	}
	type args struct {
		granularity time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			t.SetGranularity(tt.args.granularity)
		})
	}
}

//func TestTimeRangeSliceCollectionState_addRange(t1 *testing.T) {
//	type fields struct {
//		TimeRanges  []*timeRangeCollectionState
//		Granularity time.Duration
//		Order       CollectionOrder
//	}
//	type args struct {
//		timestamp time.Time
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		args   args
//		want   int
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t1.Run(tt.name, func(t1 *testing.T) {
//			t := &TimeRangeSliceCollectionState{
//				TimeRanges:  tt.fields.TimeRanges,
//				Granularity: tt.fields.Granularity,
//				Order:       tt.fields.Order,
//			}
//			if got := t.addRange(tt.args.timestamp); got != tt.want {
//				t1.Errorf("addRange() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func TestTimeRangeSliceCollectionState_mergeRangeWithNext(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*timeRangeCollectionState
		Granularity time.Duration
		Order       CollectionOrder
	}
	type args struct {
		idx int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TimeRangeSliceCollectionState{
				TimeRanges:  tt.fields.TimeRanges,
				Granularity: tt.fields.Granularity,
				Order:       tt.fields.Order,
			}
			t.mergeRangeWithNext(tt.args.idx)
		})
	}
}

//func TestTimeRangeSliceCollectionState_rangeForTime(t1 *testing.T) {
//	type fields struct {
//		TimeRanges  []*timeRangeCollectionState
//		Granularity time.Duration
//		Order       CollectionOrder
//	}
//	type args struct {
//		timestamp time.Time
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		args   args
//		want   int
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t1.Run(tt.name, func(t1 *testing.T) {
//			t := &TimeRangeSliceCollectionState{
//				TimeRanges:  tt.fields.TimeRanges,
//				Granularity: tt.fields.Granularity,
//				Order:       tt.fields.Order,
//			}
//			if got := t.rangeForTime(tt.args.timestamp); got != tt.want {
//				t1.Errorf("rangeForTime() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func TestTimeRangeSliceCollectionState_compact(t1 *testing.T) {
	tests := []struct {
		name               string
		state              *TimeRangeSliceCollectionState
		expectedRangeCount int
		expectedRanges     []*timeRangeCollectionState
	}{
		{
			name: "single_range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 5, 3, 12, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{},
						Granularity:     time.Nanosecond,
						CollectionOrder: CollectionOrderChronological,
					},
				},
			},
			expectedRangeCount: 1,
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 5, 3, 12, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{},
					Granularity:     time.Nanosecond,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "adjacent_ranges_merge",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 19, 12, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{},
						Granularity:     time.Nanosecond,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{},
						Granularity:     time.Nanosecond,
						CollectionOrder: CollectionOrderChronological,
					},
				},
			},
			expectedRangeCount: 1,
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 19, 12, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{},
					Granularity:     time.Nanosecond,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "non_adjacent_ranges_no_merge",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 19, 12, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{},
						Granularity:     time.Nanosecond,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 28, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{},
						Granularity:     time.Nanosecond,
						CollectionOrder: CollectionOrderChronological,
					},
				},
			},
			expectedRangeCount: 2,
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 19, 12, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{},
					Granularity:     time.Nanosecond,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 28, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{},
					Granularity:     time.Nanosecond,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "multiple_ranges_merge",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{},
						Granularity:     time.Nanosecond,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{},
						Granularity:     time.Nanosecond,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 20, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{},
						Granularity:     time.Nanosecond,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{},
						Granularity:     time.Nanosecond,
						CollectionOrder: CollectionOrderChronological,
					},
				},
			},
			expectedRangeCount: 2,
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{},
					Granularity:     time.Nanosecond,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 20, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{},
					Granularity:     time.Nanosecond,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
	}

	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := tt.state
			t.compact()

			if len(t.TimeRanges) != tt.expectedRangeCount {
				t1.Fatalf("compact() expected %d ranges, got %d", tt.expectedRangeCount, len(t.TimeRanges))
			}

			// Check if the ranges match by comparing each field
			for i, actual := range t.TimeRanges {
				expected := tt.expectedRanges[i]

				// Compare firstEntryTime
				if !actual.firstEntryTime.Equal(expected.firstEntryTime) {
					t1.Errorf("Range %d: firstEntryTime mismatch - expected: %v, got: %v", i, expected.firstEntryTime, actual.firstEntryTime)
				}

				// Compare lastEntryTime
				if !actual.lastEntryTime.Equal(expected.lastEntryTime) {
					t1.Errorf("Range %d: lastEntryTime mismatch - expected: %v, got: %v", i, expected.lastEntryTime, actual.lastEntryTime)
				}

				// Compare endTime
				if !actual.endTime.Equal(expected.endTime) {
					t1.Errorf("Range %d: endTime mismatch - expected: %v, got: %v", i, expected.endTime, actual.endTime)
				}
			}
		})
	}
}

func TestTimeRangeSliceCollectionState(t *testing.T) {
	collectionRunTime := time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC)
	defaultFrom := collectionRunTime.Add(-time.Hour * 24 * 7) // default from 7 days before collectionRunTime
	granularity := time.Hour

	// default single range of `2025-04-19T12:00:00` to `2025-04-26T00:00:00`
	defaultSingleRange := &timeRangeCollectionState{
		firstEntryTime:  time.Date(2025, 4, 19, 12, 0, 0, 0, time.UTC),
		lastEntryTime:   time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
		endTime:         time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
		EndObjects:      map[string]struct{}{"20250426_000000.txt": {}},
		Granularity:     granularity,
		CollectionOrder: CollectionOrderChronological,
	}

	tests := []struct {
		name  string
		state *TimeRangeSliceCollectionState
		from  time.Time
		to    time.Time
		//expectedRangesPreCompact []*timeRangeCollectionState
		expectedRanges []*timeRangeCollectionState
	}{
		{
			name:  "First Collection - Defaults (No Parameters)",
			state: nil,
			from:  defaultFrom,
			to:    collectionRunTime,
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  defaultFrom,
					lastEntryTime:   collectionRunTime,
					endTime:         collectionRunTime,
					EndObjects:      map[string]struct{}{"20250510_120000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name:  "First Collection - Default From, Custom To",
			state: nil,
			from:  defaultFrom,
			to:    time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  defaultFrom,
					lastEntryTime:   time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250505_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name:  "First Collection - Custom From, Default To",
			state: nil,
			from:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			to:    collectionRunTime,
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   collectionRunTime,
					endTime:         collectionRunTime,
					EndObjects:      map[string]struct{}{"20250510_120000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name:  "First Collection - Custom From and To",
			state: nil,
			from:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			to:    time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250505_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "One Existing Range - Defaults (No Parameters)",
			state: &TimeRangeSliceCollectionState{
				TimeRanges:  []*timeRangeCollectionState{defaultSingleRange},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: defaultSingleRange.GetEndTime(),
			to:   collectionRunTime,
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  defaultSingleRange.GetStartTime(),
					lastEntryTime:   collectionRunTime,
					endTime:         collectionRunTime,
					EndObjects:      map[string]struct{}{"20250510_120000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "One Existing Range - Collection of Non-Adjacent Range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges:  []*timeRangeCollectionState{defaultSingleRange},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 28, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				defaultSingleRange,
				{
					firstEntryTime:  time.Date(2025, 4, 28, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250503_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "One Existing Range - Collection of Adjacent Range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges:  []*timeRangeCollectionState{defaultSingleRange},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  defaultSingleRange.GetStartTime(),
					lastEntryTime:   time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250503_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "One Existing Range - Collection Encompasses Existing Range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges:  []*timeRangeCollectionState{defaultSingleRange},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250501_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "One Existing Range - Collection Overlaps Beginning of Existing Range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges:  []*timeRangeCollectionState{defaultSingleRange},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 20, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 26, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250426_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "One Existing Range - Collection Overlaps End of Existing Range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges:  []*timeRangeCollectionState{defaultSingleRange},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 24, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 19, 12, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250426_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Between Two Ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250415_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Overlapping End of First Range and Start of Second Range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 17, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250417_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Encompassing Multiple Ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 12, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 12, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250412_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250425_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Partially Overlapping Multiple Ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 17, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250417_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Collection Before All Existing Ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 3, 25, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250325_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Collection After All Existing Ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250501_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Default Collection Parameters",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 5, 10, 12, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250510_120000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Creating New Gap Between Ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 20, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 27, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 27, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250427_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250415_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 20, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 27, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 27, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250427_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Overlapping Only Some Ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 12, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 12, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250412_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 5, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 11, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 12, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 12, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250412_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Creating Multiple New Gaps",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250430_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 23, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 24, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 23, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 24, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 24, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250424_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
				{
					firstEntryTime:  time.Date(2025, 4, 25, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250430_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Exactly Adjacent to Multiple Ranges",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250415_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
		{
			name: "Multiple Existing Ranges - Partial Overlap at Exact Boundaries",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*timeRangeCollectionState{
					{
						firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250407_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
					{
						firstEntryTime:  time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC),
						lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
						EndObjects:      map[string]struct{}{"20250422_000000.txt": {}},
						Granularity:     granularity,
						CollectionOrder: CollectionOrderChronological,
					},
				},
				Granularity: granularity,
				Order:       CollectionOrderChronological,
			},
			from: time.Date(2025, 4, 7, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2025, 4, 18, 0, 0, 0, 0, time.UTC),
			expectedRanges: []*timeRangeCollectionState{
				{
					firstEntryTime:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
					lastEntryTime:   time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					endTime:         time.Date(2025, 4, 22, 0, 0, 0, 0, time.UTC),
					EndObjects:      map[string]struct{}{"20250418_000000.txt": {}},
					Granularity:     granularity,
					CollectionOrder: CollectionOrderChronological,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize the state if not provided
			if tt.state == nil {
				tt.state = NewTimeRangeSliceCollectionState(CollectionOrderChronological)
				tt.state.SetGranularity(granularity)
			}

			// TODO: We're no longer always going to recollect based on `from` so we don't need to truncate collection state - correct?

			// Simulate the collection process
			for x := tt.from; !x.After(tt.to); x = x.Add(time.Minute) {
				// Only get a file every 15 mins past hour (simplicity)
				if x.Minute()%15 != 0 {
					continue
				}
				fileName := x.Format("20060102_150405.txt")

				// Simulate adding a file to the collection state (if required - i.e.: not collected)
				activeRange := tt.state.rangeForTime(x)

				// TODO: Do I need to check if time is in next range? (i.e. a boundary time?)

				if activeRange.ShouldCollect(fileName, x) {
					err := activeRange.OnCollected(fileName, x)
					if err != nil {
						t.Errorf("Error collecting file %s at time %s: %v", fileName, x, err)
					}
				}
			}

			// Assume the collection completed successfully so compact
			tt.state.compact()

			// Check the number of ranges after compaction
			if len(tt.state.TimeRanges) != len(tt.expectedRanges) {
				t.Errorf("Expected %d ranges after compaction, got %d", len(tt.expectedRanges), len(tt.state.TimeRanges))
			}

			// Check if the ranges match by comparing each field
			for i, actual := range tt.state.TimeRanges {
				expected := tt.expectedRanges[i]

				// Compare firstEntryTime
				if !actual.firstEntryTime.Equal(expected.firstEntryTime) {
					t.Errorf("Range %d: firstEntryTime mismatch - expected: %v, got: %v", i, expected.firstEntryTime, actual.firstEntryTime)
				}

				// Compare lastEntryTime
				if !actual.lastEntryTime.Equal(expected.lastEntryTime) {
					t.Errorf("Range %d: lastEntryTime mismatch - expected: %v, got: %v", i, expected.lastEntryTime, actual.lastEntryTime)
				}

				// Compare endTime
				//if !actual.endTime.Equal(expected.endTime) {
				//	t.Errorf("Range %d: endTime mismatch - expected: %v, got: %v", i, expected.endTime, actual.endTime)
				//}

				// Compare EndObjects
				//if !reflect.DeepEqual(actual.EndObjects, expected.EndObjects) {
				//	t.Errorf("Range %d: EndObjects mismatch - expected: %v, got: %v", i, expected.EndObjects, actual.EndObjects)
				//}
			}

		})
	}
}
