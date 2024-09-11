package collection_state

import (
	"testing"
	"time"

	a "github.com/stretchr/testify/assert"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
)

// TODO: #collectionState - Add AddRange() to interface to avoid casting

var (
	start2023    = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	end2023      = time.Date(2023, 12, 31, 23, 59, 59, 999999, time.UTC)
	startFeb2024 = time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	endFeb2024   = time.Date(2024, 2, 29, 23, 59, 59, 999999, time.UTC)
)

var singleRangeState = &GenericCollectionState[parse.Config]{
	Ranges: []*CollectionStateRange{
		{
			StartTime:        start2023,
			EndTime:          end2023,
			StartIdentifiers: map[string]any{"1": struct{}{}},
			EndIdentifiers:   map[string]any{"1000": struct{}{}},
		},
	},
}

var multipleRangeState = &GenericCollectionState[parse.Config]{
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
			s:    singleRangeState,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a.Equalf(t, tt.want, tt.s.IsEmpty(), "IsEmpty()")
		})
	}
}

func TestGenericCollectionState_ShouldCollect(t *testing.T) {
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
			s:    singleRangeState,
			args: args{ts: start2023.Add(3624 * time.Hour), key: "500"},
			want: false,
		},
		{
			name: "GenericCollectionState ShouldCollect True When Not In An Existing Range",
			s:    singleRangeState,
			args: args{ts: end2023.Add(2 * time.Second), key: "1001"},
			want: true,
		},
		{
			name: "GenericCollectionState ShouldCollect False When At Range Boundary With Existing Key",
			s:    singleRangeState,
			args: args{ts: end2023, key: "1000"},
			want: false,
		},
		{
			name: "GenericCollectionState ShouldCollect True When At Range Boundary With Existing Key",
			s:    singleRangeState,
			args: args{ts: end2023, key: "1000a"},
			want: true,
		},
		{
			name: "GenericCollectionState ShouldCollect True When In Between Ranges",
			s:    multipleRangeState,
			args: args{ts: end2023.Add(18 * time.Hour), key: "1111"},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a.Equalf(t, tt.want, tt.s.ShouldCollect(tt.args.ts, tt.args.key), "ShouldCollect(%v, %v)", tt.args.ts, tt.args.key)
		})
	}
}

//func TestGenericCollectionState_Upsert(t *testing.T) {
//	type args struct {
//		ts   time.Time
//		key  string
//		meta any
//	}
//	type testCase[T parse.Config] struct {
//		name string
//		s    *GenericCollectionState[T]
//		args args
//	}
//	tests := []testCase[parse.Config]{
//		{},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			tt.s.Upsert(tt.args.ts, tt.args.key, tt.args.meta)
//		})
//	}
//}

//func TestGenericCollectionState_StartCollection(t *testing.T) {
//	type testCase[T parse.Config] struct {
//		name string
//		s    GenericCollectionState[T]
//	}
//	tests := []testCase[ /* TODO: Insert concrete types here */ ]{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			tt.s.StartCollection()
//		})
//	}
//}

//func TestGenericCollectionState_EndCollection(t *testing.T) {
//	type testCase[T parse.Config] struct {
//		name string
//		s    GenericCollectionState[T]
//	}
//	tests := []testCase[ /* TODO: Insert concrete types here */ ]{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			tt.s.EndCollection()
//		})
//	}
//}
