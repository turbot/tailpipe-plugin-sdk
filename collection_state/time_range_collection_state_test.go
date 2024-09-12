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

func singleRangeState() *TimeRangeCollectionState[parse.Config] {
	return &TimeRangeCollectionState[parse.Config]{
		Ranges: []*CollectionStateTimeRange{
			{
				StartTime:        start2023,
				EndTime:          end2023,
				StartIdentifiers: map[string]any{"1": struct{}{}},
				EndIdentifiers:   map[string]any{"1000": struct{}{}},
			},
		},
	}
}

func multiRangeState() *TimeRangeCollectionState[parse.Config] {
	return &TimeRangeCollectionState[parse.Config]{
		Ranges: []*CollectionStateTimeRange{
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

func TestTimeRangeCollectionState_IsEmpty(t *testing.T) {
	type testCase[T parse.Config] struct {
		name string
		s    *TimeRangeCollectionState[T]
		want bool
	}
	tests := []testCase[parse.Config]{
		{
			name: "Returns true when no ranges exist",
			s:    &TimeRangeCollectionState[parse.Config]{},
			want: true,
		},
		{
			name: "Returns false when ranges exist",
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

func TestTimeRangeCollectionState_ShouldCollectRow(t *testing.T) {
	type args struct {
		ts  time.Time
		key string
	}
	type testCase[T parse.Config] struct {
		name string
		s    *TimeRangeCollectionState[T]
		args args
		want bool
	}
	tests := []testCase[parse.Config]{
		{
			name: "Returns false when ts inside an existing range",
			s:    singleRangeState(),
			args: args{ts: start2023.Add(3624 * time.Hour), key: "500"},
			want: false,
		},
		{
			name: "Returns true when ts outside an existing range",
			s:    singleRangeState(),
			args: args{ts: end2023.Add(2 * time.Second), key: "1001"},
			want: true,
		},
		{
			name: "Returns false when ts is boundary of an existing range with existing key",
			s:    singleRangeState(),
			args: args{ts: end2023, key: "1000"},
			want: false,
		},
		{
			name: "Returns true when ts is boundary of an existing range with new key",
			s:    singleRangeState(),
			args: args{ts: end2023, key: "1000a"},
			want: true,
		},
		{
			name: "Returns true when ts is between existing ranges",
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

func TestTimeRangeCollectionState_Upsert(t *testing.T) {
	type args struct {
		ts   time.Time
		key  string
		meta any
	}
	type testCase[T parse.Config] struct {
		name                 string
		s                    *TimeRangeCollectionState[T]
		initialCurrentRange  *CollectionStateTimeRange
		args                 args
		expectedCurrentRange *CollectionStateTimeRange
	}
	tests := []testCase[parse.Config]{
		{
			name:                "Sets all fields when currentRange is empty",
			s:                   &TimeRangeCollectionState[parse.Config]{},
			args:                args{ts: start2023, key: "1", meta: nil},
			initialCurrentRange: NewCollectionStateTimeRange(),
			expectedCurrentRange: &CollectionStateTimeRange{
				StartTime:        start2023,
				EndTime:          start2023,
				StartIdentifiers: map[string]any{"1": struct{}{}},
				EndIdentifiers:   map[string]any{"1": struct{}{}},
			},
		},
		{
			name:                "Sets StartTime and StartIdentifiers when ts before StartTime",
			s:                   singleRangeState(),
			args:                args{ts: start2023.Add(-1 * time.Hour), key: "0", meta: nil},
			initialCurrentRange: singleRangeState().Ranges[0],
			expectedCurrentRange: &CollectionStateTimeRange{
				StartTime:        start2023.Add(-1 * time.Hour),
				EndTime:          end2023,
				StartIdentifiers: map[string]any{"0": struct{}{}},
				EndIdentifiers:   map[string]any{"1000": struct{}{}},
			},
		},
		{
			name:                "Sets EndTime and EndIdentifiers when ts after EndTime",
			s:                   singleRangeState(),
			args:                args{ts: end2023.Add(1 * time.Hour), key: "1001", meta: nil},
			initialCurrentRange: singleRangeState().Ranges[0],
			expectedCurrentRange: &CollectionStateTimeRange{
				StartTime:        start2023,
				EndTime:          end2023.Add(1 * time.Hour),
				StartIdentifiers: map[string]any{"1": struct{}{}},
				EndIdentifiers:   map[string]any{"1001": struct{}{}},
			},
		},
		{
			name:                "Sets StartIdentifiers when ts equals StartTime",
			s:                   singleRangeState(),
			args:                args{ts: start2023, key: "1a", meta: nil},
			initialCurrentRange: singleRangeState().Ranges[0],
			expectedCurrentRange: &CollectionStateTimeRange{
				StartTime:        start2023,
				EndTime:          end2023,
				StartIdentifiers: map[string]any{"1": struct{}{}, "1a": struct{}{}},
				EndIdentifiers:   map[string]any{"1000": struct{}{}},
			},
		},
		{
			name:                "Sets EndIdentifiers when ts equals EndTime",
			s:                   singleRangeState(),
			args:                args{ts: end2023, key: "1000a", meta: nil},
			initialCurrentRange: singleRangeState().Ranges[0],
			expectedCurrentRange: &CollectionStateTimeRange{
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

func TestTimeRangeCollectionState_StartCollection(t *testing.T) {
	type args struct {
		HasContinuation bool
		IsChronological bool
	}
	type testCase[T parse.Config] struct {
		name                 string
		s                    *TimeRangeCollectionState[T]
		args                 args
		expectedCount        int
		expectedMergeRange   *CollectionStateTimeRange
		expectedCurrentRange *CollectionStateTimeRange
	}
	tests := []testCase[parse.Config]{
		{
			name:               "Adds new range when no ranges exist",
			s:                  &TimeRangeCollectionState[parse.Config]{},
			args:               args{IsChronological: false, HasContinuation: false},
			expectedCount:      1,
			expectedMergeRange: nil,
			expectedCurrentRange: &CollectionStateTimeRange{
				StartIdentifiers: make(map[string]any),
				EndIdentifiers:   make(map[string]any),
			},
		},
		{
			name:               "Sets mergeRange to latest and currentRange to new when reverse chronological",
			s:                  multiRangeState(),
			args:               args{IsChronological: false, HasContinuation: false},
			expectedCount:      3,
			expectedMergeRange: multiRangeState().Ranges[1],
			expectedCurrentRange: &CollectionStateTimeRange{
				StartIdentifiers: make(map[string]any),
				EndIdentifiers:   make(map[string]any),
			},
		},
		{
			name:                 "Sets mergeRange nil and currentRange to latest when chronological with continuation",
			s:                    multiRangeState(),
			args:                 args{IsChronological: true, HasContinuation: true},
			expectedCount:        2,
			expectedMergeRange:   nil,
			expectedCurrentRange: multiRangeState().Ranges[1],
		},
		{
			name:                 "Sets mergeRange nil and currentRange to earliest when chronological without continuation",
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

func TestTimeRangeCollectionState_EndCollection(t *testing.T) {
	type testCase[T parse.Config] struct {
		name string
		s    *TimeRangeCollectionState[T]
	}
	tests := []testCase[parse.Config]{
		{
			name: "Returns without error when mergeRange is nil",
			s:    &TimeRangeCollectionState[parse.Config]{},
		},
		// TODO: #test add test for ensuring currentRange is merged with mergeRange
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.EndCollection()
		})
	}
}

func TestTimeRangeCollectionState_GetLatestEndTime(t *testing.T) {
	type testCase[T parse.Config] struct {
		name string
		s    *TimeRangeCollectionState[T]
		want *time.Time
	}
	tests := []testCase[parse.Config]{
		{
			name: "Returns nil when no ranges exist",
			s:    &TimeRangeCollectionState[parse.Config]{},
			want: nil,
		},
		{
			name: "Returns latest end time when single range exists",
			s:    singleRangeState(),
			want: &end2023,
		},
		{
			name: "Returns latest end time when multiple ranges exist",
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

func TestTimeRangeCollectionState_GetEarliestStartTime(t *testing.T) {
	type testCase[T parse.Config] struct {
		name string
		s    *TimeRangeCollectionState[T]
		want *time.Time
	}
	tests := []testCase[parse.Config]{
		{
			name: "Returns nil when no ranges exist",
			s:    &TimeRangeCollectionState[parse.Config]{},
			want: nil,
		},
		{
			name: "Returns earliest start time when single range exists",
			s:    singleRangeState(),
			want: &start2023,
		},
		{
			name: "Returns earliest start time when multiple ranges exist",
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

func TestTimeRangeCollectionState_mergeRanges(t *testing.T) {
	type testCase[T parse.Config] struct {
		name          string
		s             *TimeRangeCollectionState[T]
		want          []*CollectionStateTimeRange
		expectedCount int
	}
	tests := []testCase[parse.Config]{
		{
			name:          "returns empty when no ranges exist",
			s:             &TimeRangeCollectionState[parse.Config]{},
			want:          []*CollectionStateTimeRange(nil),
			expectedCount: 0,
		},
		{
			name:          "Returns unchanged Ranges when mergeRange is nil",
			s:             singleRangeState(),
			want:          singleRangeState().Ranges,
			expectedCount: 1,
		},
		{
			name: "Returns merged range when mergeRange is set",
			s:    multiRangeState(),
			want: []*CollectionStateTimeRange{
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
