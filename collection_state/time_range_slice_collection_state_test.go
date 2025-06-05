package collection_state

import (
	"reflect"
	"testing"
	"time"
)

func TestTimeRangeSliceCollectionState_GetEndTime(t1 *testing.T) {
	type fields struct {
		TimeRanges  []*TimeRangeCollectionStateImpl
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
				TimeRanges:  []*TimeRangeCollectionStateImpl{},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: time.Time{},
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
		TimeRanges  []*TimeRangeCollectionStateImpl
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
		TimeRanges  []*TimeRangeCollectionStateImpl
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
				TimeRanges:  []*TimeRangeCollectionStateImpl{},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: time.Time{},
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
		TimeRanges  []*TimeRangeCollectionStateImpl
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
				TimeRanges:  []*TimeRangeCollectionStateImpl{},
				Granularity: time.Hour,
				Order:       CollectionOrderChronological,
			},
			want: true,
		},
		{
			name: "single_range",
			fields: fields{
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
		TimeRanges  []*TimeRangeCollectionStateImpl
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
		TimeRanges  []*TimeRangeCollectionStateImpl
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
//		TimeRanges  []*TimeRangeCollectionStateImpl
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
		TimeRanges  []*TimeRangeCollectionStateImpl
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
//		TimeRanges  []*TimeRangeCollectionStateImpl
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
		expectedRanges     []*TimeRangeCollectionStateImpl
	}{
		{
			name: "single_range",
			state: &TimeRangeSliceCollectionState{
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
			expectedRanges: []*TimeRangeCollectionStateImpl{
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
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
			expectedRanges: []*TimeRangeCollectionStateImpl{
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
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
			expectedRanges: []*TimeRangeCollectionStateImpl{
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
				TimeRanges: []*TimeRangeCollectionStateImpl{
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
			expectedRanges: []*TimeRangeCollectionStateImpl{
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
