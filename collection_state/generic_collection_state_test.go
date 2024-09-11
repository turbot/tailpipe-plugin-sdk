package collection_state

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
)

var (
	start2023    = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	end2023      = time.Date(2023, 12, 31, 23, 59, 59, 999999, time.UTC)
	startFeb2024 = time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	endFeb2024   = time.Date(2024, 2, 29, 23, 59, 59, 999999, time.UTC)
)

func singleRangeState() *GenericCollectionState[parse.Config] {
	return &GenericCollectionState[parse.Config]{
		Ranges: []*CollectionStateRange{
			{
				StartTime:        start2023,
				EndTime:          end2023,
				StartIdentifiers: map[string]any{"1": struct{}{}},
				EndIdentifiers:   map[string]any{"1000": struct{}{}},
			},
		},
	}
}

func multiRangeState() *GenericCollectionState[parse.Config] {
	return &GenericCollectionState[parse.Config]{
		Ranges: []*CollectionStateRange{
			{
				StartTime:        start2023,
				EndTime:          end2023,
				StartIdentifiers: map[string]any{"1": struct{}{}},
				EndIdentifiers:   map[string]any{"1000": struct{}{}},
			},
			{
				StartTime:        startFeb2024,
				EndTime:          endFeb2024,
				StartIdentifiers: map[string]any{"1200": struct{}{}},
				EndIdentifiers:   map[string]any{"1300": struct{}{}},
			},
		},
	}
}

func TestGenericCollectionState_IsEmpty(t *testing.T) {
	type testCase[T parse.Config] struct {
		name string
		s    *GenericCollectionState[T]
		want bool
	}
	tests := []testCase[parse.Config]{
		{
			name: "GenericCollectionState IsEmpty True When No Ranges Exist",
			s:    &GenericCollectionState[parse.Config]{},
			want: true,
		},
		{
			name: "GenericCollectionState IsEmpty False When Ranges Exist",
			s:    singleRangeState(),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.s.IsEmpty(), "IsEmpty()")
		})
	}
}

func TestGenericCollectionState_ShouldCollectRow(t *testing.T) {
	type args struct {
		ts  time.Time
		key string
	}
	type testCase[T parse.Config] struct {
		name string
		s    *GenericCollectionState[T]
		args args
		want bool
	}
	tests := []testCase[parse.Config]{
		{
			name: "GenericCollectionState ShouldCollect False When Inside An Existing Range",
			s:    singleRangeState(),
			args: args{ts: start2023.Add(3624 * time.Hour), key: "500"},
			want: false,
		},
		{
			name: "GenericCollectionState ShouldCollect True When Not In An Existing Range",
			s:    singleRangeState(),
			args: args{ts: end2023.Add(2 * time.Second), key: "1001"},
			want: true,
		},
		{
			name: "GenericCollectionState ShouldCollect False When At Range Boundary With Existing Key",
			s:    singleRangeState(),
			args: args{ts: end2023, key: "1000"},
			want: false,
		},
		{
			name: "GenericCollectionState ShouldCollect True When At Range Boundary With Existing Key",
			s:    singleRangeState(),
			args: args{ts: end2023, key: "1000a"},
			want: true,
		},
		{
			name: "GenericCollectionState ShouldCollect True When In Between Ranges",
			s:    multiRangeState(),
			args: args{ts: end2023.Add(18 * time.Hour), key: "1111"},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.s.ShouldCollectRow(tt.args.ts, tt.args.key), "ShouldCollectRow(%v, %v)", tt.args.ts, tt.args.key)
		})
	}
}

func TestGenericCollectionState_Upsert(t *testing.T) {
	type args struct {
		ts   time.Time
		key  string
		meta any
	}
	type testCase[T parse.Config] struct {
		name                 string
		s                    *GenericCollectionState[T]
		initialCurrentRange  *CollectionStateRange
		args                 args
		expectedCurrentRange *CollectionStateRange
	}
	tests := []testCase[parse.Config]{
		{
			name:                "GenericCollectionState Upsert Sets StartTime And EndTime When CurrentRange Is New (Empty)",
			s:                   &GenericCollectionState[parse.Config]{},
			args:                args{ts: start2023, key: "1", meta: nil},
			initialCurrentRange: NewCollectionStateRange(),
			expectedCurrentRange: &CollectionStateRange{
				StartTime:        start2023,
				EndTime:          start2023,
				StartIdentifiers: map[string]any{"1": struct{}{}},
				EndIdentifiers:   map[string]any{"1": struct{}{}},
			},
		},
		{
			name:                "GenericCollectionState Upsert Sets StartTime And StartIdentifiers When ts Before StartTime",
			s:                   singleRangeState(),
			args:                args{ts: start2023.Add(-1 * time.Hour), key: "0", meta: nil},
			initialCurrentRange: singleRangeState().Ranges[0],
			expectedCurrentRange: &CollectionStateRange{
				StartTime:        start2023.Add(-1 * time.Hour),
				EndTime:          end2023,
				StartIdentifiers: map[string]any{"0": struct{}{}},
				EndIdentifiers:   map[string]any{"1000": struct{}{}},
			},
		},
		{
			name:                "GenericCollectionState Upsert Sets EndTime And EndIdentifiers When ts After EndTime",
			s:                   singleRangeState(),
			args:                args{ts: end2023.Add(1 * time.Hour), key: "1001", meta: nil},
			initialCurrentRange: singleRangeState().Ranges[0],
			expectedCurrentRange: &CollectionStateRange{
				StartTime:        start2023,
				EndTime:          end2023.Add(1 * time.Hour),
				StartIdentifiers: map[string]any{"1": struct{}{}},
				EndIdentifiers:   map[string]any{"1001": struct{}{}},
			},
		},
		{
			name:                "GenericCollectionState Upsert Sets StartIdentifiers When ts Equals StartTime",
			s:                   singleRangeState(),
			args:                args{ts: start2023, key: "1a", meta: nil},
			initialCurrentRange: singleRangeState().Ranges[0],
			expectedCurrentRange: &CollectionStateRange{
				StartTime:        start2023,
				EndTime:          end2023,
				StartIdentifiers: map[string]any{"1": struct{}{}, "1a": struct{}{}},
				EndIdentifiers:   map[string]any{"1000": struct{}{}},
			},
		},
		{
			name:                "GenericCollectionState Upsert Sets EndIdentifiers When ts Equals EndTime",
			s:                   singleRangeState(),
			args:                args{ts: end2023, key: "1000a", meta: nil},
			initialCurrentRange: singleRangeState().Ranges[0],
			expectedCurrentRange: &CollectionStateRange{
				StartTime:        start2023,
				EndTime:          end2023,
				StartIdentifiers: map[string]any{"1": struct{}{}},
				EndIdentifiers:   map[string]any{"1000": struct{}{}, "1000a": struct{}{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.currentRange = tt.initialCurrentRange
			tt.s.Upsert(tt.args.ts, tt.args.key, tt.args.meta)

			assert.Equalf(t, tt.expectedCurrentRange, tt.s.currentRange, "CurrentRange")
		})
	}
}

func TestGenericCollectionState_StartCollection(t *testing.T) {
	type args struct {
		HasContinuation bool
		IsChronological bool
	}
	type testCase[T parse.Config] struct {
		name                 string
		s                    *GenericCollectionState[T]
		args                 args
		expectedCount        int
		expectedMergeRange   *CollectionStateRange
		expectedCurrentRange *CollectionStateRange
	}
	tests := []testCase[parse.Config]{
		{
			name:               "GenericCollectionState StartCollection Adds New Range When No Ranges Exist",
			s:                  &GenericCollectionState[parse.Config]{},
			args:               args{IsChronological: false, HasContinuation: false},
			expectedCount:      1,
			expectedMergeRange: nil,
			expectedCurrentRange: &CollectionStateRange{
				StartIdentifiers: make(map[string]any),
				EndIdentifiers:   make(map[string]any),
			},
		},
		{
			name:               "GenericCollectionState StartCollection When ReverseChronological Has Latest Range As Merge Range New Current Range",
			s:                  multiRangeState(),
			args:               args{IsChronological: false, HasContinuation: false},
			expectedCount:      3,
			expectedMergeRange: multiRangeState().Ranges[1],
			expectedCurrentRange: &CollectionStateRange{
				StartIdentifiers: make(map[string]any),
				EndIdentifiers:   make(map[string]any),
			},
		},
		{
			name:                 "GenericCollectionState StartCollection When Chronological Having Continuation Has Latest Range As Current Range No Merge Range",
			s:                    multiRangeState(),
			args:                 args{IsChronological: true, HasContinuation: true},
			expectedCount:        2,
			expectedMergeRange:   nil,
			expectedCurrentRange: multiRangeState().Ranges[1],
		},
		{
			name:                 "GenericCollectionState StartCollection When Chronological No Continuation Has Earliest Range As Current Range No Merge Range",
			s:                    multiRangeState(),
			args:                 args{IsChronological: true, HasContinuation: false},
			expectedCount:        2,
			expectedMergeRange:   nil,
			expectedCurrentRange: multiRangeState().Ranges[0],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.IsChronological = tt.args.IsChronological
			tt.s.HasContinuation = tt.args.HasContinuation
			tt.s.StartCollection()
			assert.Equalf(t, tt.expectedCount, len(tt.s.Ranges), "len(Ranges)")
			assert.Equalf(t, tt.expectedMergeRange, tt.s.mergeRange, "MergeRange")
			assert.Equalf(t, tt.expectedCurrentRange, tt.s.currentRange, "CurrentRange")
		})
	}
}

func TestGenericCollectionState_EndCollection(t *testing.T) {
	type testCase[T parse.Config] struct {
		name string
		s    *GenericCollectionState[T]
	}
	tests := []testCase[parse.Config]{
		{
			name: "GenericCollectionState EndCollection Returns Successfully When MergeRange Is Nil",
			s:    &GenericCollectionState[parse.Config]{},
		},
		// TODO: #test add test for ensuring currentRange is merged with mergeRange
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.EndCollection()
		})
	}
}

func TestGenericCollectionState_GetLatestEndTime(t *testing.T) {
	type testCase[T parse.Config] struct {
		name string
		s    *GenericCollectionState[T]
		want *time.Time
	}
	tests := []testCase[parse.Config]{
		{
			name: "GenericCollectionState GetLatestEndTime Returns Nil When No Ranges Exist",
			s:    &GenericCollectionState[parse.Config]{},
			want: nil,
		},
		{
			name: "GenericCollectionState GetLatestEndTime Returns Latest EndTime When Single Range Exists",
			s:    singleRangeState(),
			want: &end2023,
		},
		{
			name: "GenericCollectionState GetLatestEndTime Returns Latest EndTime When Multiple Ranges Exist",
			s:    multiRangeState(),
			want: &endFeb2024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.s.GetLatestEndTime(), "GetLatestEndTime()")
		})
	}
}

func TestGenericCollectionState_GetEarliestStartTime(t *testing.T) {
	type testCase[T parse.Config] struct {
		name string
		s    *GenericCollectionState[T]
		want *time.Time
	}
	tests := []testCase[parse.Config]{
		{
			name: "GenericCollectionState GetEarliestStartTime Returns Nil When No Ranges Exist",
			s:    &GenericCollectionState[parse.Config]{},
			want: nil,
		},
		{
			name: "GenericCollectionState GetEarliestStartTime Returns Earliest StartTime When Single Range Exists",
			s:    singleRangeState(),
			want: &start2023,
		},
		{
			name: "GenericCollectionState GetEarliestStartTime Returns Earliest StartTime When Multiple Ranges Exist",
			s:    multiRangeState(),
			want: &start2023,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.s.GetEarliestStartTime(), "GetEarliestStartTime()")
		})
	}
}

func TestGenericCollectionState_mergeRanges(t *testing.T) {
	type testCase[T parse.Config] struct {
		name          string
		s             *GenericCollectionState[T]
		want          []*CollectionStateRange
		expectedCount int
	}
	tests := []testCase[parse.Config]{
		{
			name:          "GenericCollectionState mergeRanges Returns Empty When No Ranges Exist",
			s:             &GenericCollectionState[parse.Config]{},
			want:          []*CollectionStateRange(nil),
			expectedCount: 0,
		},
		{
			name:          "GenericCollectionState mergeRanges Returns Unchanged Ranges When MergeRange Not Set",
			s:             singleRangeState(),
			want:          []*CollectionStateRange{singleRangeState().Ranges[0]},
			expectedCount: 1,
		},
		{
			name: "GenericCollectionState mergeRanges Returns Merged Ranges When MergeRange Set",
			s:    multiRangeState(),
			want: []*CollectionStateRange{
				{
					StartTime:        start2023,
					EndTime:          endFeb2024,
					StartIdentifiers: map[string]any{"1": struct{}{}},
					EndIdentifiers:   map[string]any{"1300": struct{}{}},
				},
			},
			expectedCount: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch len(tt.s.Ranges) {
			case 0:
				tt.s.mergeRange = nil
				tt.s.currentRange = nil
			case 1:
				tt.s.mergeRange = nil
				tt.s.currentRange = tt.s.Ranges[0]
			default:
				tt.s.mergeRange = tt.s.getEarliestRange()
				tt.s.currentRange = tt.s.getLatestRange()
			}

			out := tt.s.mergeRanges()
			assert.Equalf(t, tt.want, out, "mergeRanges()")
			assert.Equalf(t, tt.expectedCount, len(out), "len(Ranges)")
		})
	}
}
